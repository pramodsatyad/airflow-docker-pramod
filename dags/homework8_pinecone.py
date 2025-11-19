from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import task
from airflow.models import Variable
import pandas as pd
import time
import os
import json
import ast
import requests

from sentence_transformers import SentenceTransformer
from pinecone import Pinecone, ServerlessSpec

# ---------- Defaults ----------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='TMDB_to_Pinecone',
    default_args=default_args,
    description='Build a movie search engine using Pinecone from TMDB 5000 dataset',
    schedule=timedelta(days=7),
    start_date=datetime(2025, 11, 18),
    catchup=False,
    tags=['tmdb', 'pinecone', 'search-engine'],
) as dag:
    """
    DAG to build a movie search engine using the TMDB 5000 dataset + Pinecone
    """

    # ---- Constants ----
    DOWNLOAD_URL = 'https://grepp-reco-test.s3.ap-northeast-2.amazonaws.com/tmdb_5000_movies.csv'
    DATA_DIR = '/tmp/tmdb_data'
    DOWNLOADED_CSV = f'{DATA_DIR}/tmdb_5000_movies.csv'
    PREPROCESSED_PATH = f'{DATA_DIR}/tmdb_preprocessed.csv'

    INDEX_NAME = 'tmdb-semantic-search'
    MODEL_NAME = 'all-MiniLM-L6-v2'
    DIM = 384  # all-MiniLM-L6-v2 output size

    @task
    def download_data() -> str:
        """Download TMDB dataset from S3 with requests and save locally."""
        os.makedirs(DATA_DIR, exist_ok=True)

        resp = requests.get(DOWNLOAD_URL, stream=True, timeout=60)
        if resp.status_code != 200:
            raise Exception(f"Failed to download: HTTP {resp.status_code}")

        with open(DOWNLOADED_CSV, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

        # quick sanity check
        df = pd.read_csv(DOWNLOADED_CSV)
        print(f"Downloaded TMDB CSV to {DOWNLOADED_CSV} with {len(df)} rows")
        return DOWNLOADED_CSV

    @task
    def preprocess_data(data_path: str) -> str:
        """Clean and prepare movie texts + metadata for embedding."""
        df = pd.read_csv(data_path)

        def parse_obj_list(val):
            if not isinstance(val, str):
                return []
            try:
                data = json.loads(val)
            except Exception:
                try:
                    data = ast.literal_eval(val)
                except Exception:
                    return []
            names = [d.get("name") for d in data if isinstance(d, dict) and "name" in d]
            return [n for n in names if n]

        for c in ['title', 'original_title', 'overview', 'genres', 'keywords', 'release_date', 'id']:
            if c not in df.columns:
                df[c] = ""

        rows = []
        for _, r in df.iterrows():
            title = str(r.get('title') or r.get('original_title') or '').strip()
            overview = str(r.get('overview') or '').strip()
            genres = parse_obj_list(r.get('genres', ''))
            keywords = parse_obj_list(r.get('keywords', ''))
            release_date = str(r.get('release_date') or '')

            if not title and not overview:
                continue

            parts = []
            if title:
                parts.append(f"Title: {title}")
            if overview:
                parts.append(f"Overview: {overview}")
            if genres:
                parts.append("Genres: " + ", ".join(genres[:6]))
            if keywords:
                parts.append("Keywords: " + ", ".join(keywords[:10]))
            text = " | ".join(parts)

            movie_id = r.get('id')
            try:
                movie_id = str(int(movie_id))
            except Exception:
                movie_id = str(movie_id)

            meta = {
                'title': title,
                'release_date': release_date,
                'movie_id': movie_id,
            }

            rows.append({
                'id': f'movie-{movie_id}',
                'text': text,
                'metadata': json.dumps(meta),
            })

        out_df = pd.DataFrame(rows)
        os.makedirs(os.path.dirname(PREPROCESSED_PATH), exist_ok=True)
        out_df.to_csv(PREPROCESSED_PATH, index=False)
        print(f"Preprocessed data saved to {PREPROCESSED_PATH} with {len(out_df)} rows")
        return PREPROCESSED_PATH

    @task
    def create_pinecone_index() -> str:
        """Create (or reset) Pinecone index."""
        api_key = Variable.get("pinecone_api_key")
        pc = Pinecone(api_key=api_key)

        spec = ServerlessSpec(cloud="aws", region="us-east-1")

        existing = [info["name"] for info in pc.list_indexes()]
        if INDEX_NAME in existing:
            pc.delete_index(INDEX_NAME)

        pc.create_index(
            name=INDEX_NAME,
            dimension=DIM,
            metric='dotproduct',  # using normalized embeddings → dot ≈ cosine
            spec=spec,
        )

        while not pc.describe_index(INDEX_NAME).status['ready']:
            time.sleep(1)

        print(f"Pinecone index '{INDEX_NAME}' ready")
        return INDEX_NAME

    @task
    def generate_embeddings_and_upsert(preprocessed_path: str, index_name: str) -> str:
        """Generate embeddings and upsert to Pinecone in batches."""
        api_key = Variable.get("pinecone_api_key")
        df = pd.read_csv(preprocessed_path)

        model = SentenceTransformer(MODEL_NAME, device='cpu')

        pc = Pinecone(api_key=api_key)
        index = pc.Index(index_name)

        batch_size = 200
        total = len(df)
        for i in range(0, total, batch_size):
            batch = df.iloc[i:i+batch_size].copy()
            texts = batch['text'].astype(str).tolist()
            metas = batch['metadata'].apply(lambda s: json.loads(s)).tolist()
            ids = batch['id'].astype(str).tolist()

            embs = model.encode(texts, batch_size=64, normalize_embeddings=True)

            upserts = [{
                'id': ids[j],
                'values': embs[j].tolist(),
                'metadata': metas[j]
            } for j in range(len(batch))]

            index.upsert(upserts)
            print(f"Upserted {len(upserts)} vectors ({i+len(upserts)}/{total})")

        print(f"Successfully upserted {total} records to '{index_name}'")
        return index_name

    @task
    def test_search_query(index_name: str):
        """Run sample queries and print top matches."""
        api_key = Variable.get("pinecone_api_key")
        pc = Pinecone(api_key=api_key)
        index = pc.Index(index_name)

        model = SentenceTransformer(MODEL_NAME, device='cpu')

        queries = [
            "epic space adventure with aliens",
            "heartwarming family animation",
            "gritty crime drama in a big city",
        ]

        for q in queries:
            qvec = model.encode(q, normalize_embeddings=True).tolist()
            res = index.query(vector=qvec, top_k=5, include_metadata=True)
            print(f"\nSearch results for: '{q}'")
            for m in res['matches']:
                title = (m['metadata'] or {}).get('title', '')
                print(f"ID: {m['id']}, Score: {m['score']:.4f}, Title: {title[:70]}...")

    # ---- Task order (TaskFlow API) ----
    data_path = download_data()
    preprocessed_path = preprocess_data(data_path)
    index_name = create_pinecone_index()
    final_index = generate_embeddings_and_upsert(preprocessed_path, index_name)
    test_search_query(final_index)
