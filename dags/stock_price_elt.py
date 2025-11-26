# dags/two_stock_simple.py

from datetime import datetime, timedelta
import pandas as pd
import yfinance as yf

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator



# ---------------------------
# Snowflake connection (via Airflow Connection)
# ---------------------------
def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()  


# ---------------------------
# EXTRACT
# ---------------------------
@task
def extract():
    """
    Download raw yfinance data and return it as {symbol: [raw-records]}.
    Ensures columns are single-level and JSON-serializable.
    """
    symbols_csv = Variable.get("stock_symbols", default_var="NVDA,AAPL")
    lookback_days = int(Variable.get("lookback_days", default_var="180"))
    symbols = [s.strip() for s in symbols_csv.split(",") if s.strip()]

    out = {}
    for sym in symbols:
        df = yf.download(
            sym,
            period=f"{lookback_days}d",
            interval="1d",
            auto_adjust=False,
            progress=False,
            threads=False,       # avoid cookie/SQLite issue
            group_by="column",   # ensure non-MultiIndex columns
        )

        if df is None or df.empty:
            out[sym] = []
            continue

        df = df.reset_index()

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]

        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"]).dt.tz_localize(None).dt.date.astype(str)

        out[sym] = df.to_dict(orient="records")

    return out



@task
def transform(raw):
    """
    Turn raw yfinance records into rows ready for INSERT:
    [SYMBOL, DATE, OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME]
    """
    rows = []
    for sym, recs in raw.items():
        if not recs:
            continue

        df = pd.DataFrame(recs)

        needed = {"Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"}
        if not needed.issubset(df.columns):
            continue
        df = df.dropna(subset=["Close"])
        df = df.rename(columns={"Date": "DATE","Open": "OPEN","High": "HIGH","Low": "LOW",
            "Close": "CLOSE","Adj Close": "ADJ_CLOSE","Volume": "VOLUME", })

        df["DATE"] = pd.to_datetime(df["DATE"]).dt.tz_localize(None).dt.date.astype(str)

        df = df[(df["LOW"] <= df["HIGH"]) & (df["VOLUME"] >= 0)]

        df["SYMBOL"] = sym
        df = df[["SYMBOL", "DATE", "OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE", "VOLUME"]]

        df = df.where(pd.notna(df), None)

        rows.extend(df.values.tolist())

    return rows


# ---------------------------
# LOAD
# ---------------------------
@task
def load(rows, target_table: str):
    """
    Idempotent load:
      - Create target if needed
      - Create a TEMP staging table
      - Insert batch into staging
      - Abort if staging is empty
      - MERGE staging -> target (upsert)
    """
    ddl_target = f"""
    CREATE TABLE IF NOT EXISTS {target_table} (
      SYMBOL       VARCHAR(16)   NOT NULL,
      "DATE"       DATE          NOT NULL,
      OPEN         NUMBER(18,6),
      HIGH         NUMBER(18,6),
      LOW          NUMBER(18,6),
      CLOSE        NUMBER(18,6),
      VOLUME       NUMBER(38,0),
      ADJ_CLOSE    NUMBER(18,6),
      SOURCE       VARCHAR(32)    DEFAULT 'yfinance',
      LOAD_TS      TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
      PRIMARY KEY (SYMBOL, "DATE")
    );
    """

    # temp staging table name
    stage = target_table + "_STAGE"

    ddl_stage = f"""
    CREATE TEMPORARY TABLE {stage} LIKE {target_table};
    """

    insert_stage_sql = f"""
    INSERT INTO {stage}
      (SYMBOL, "DATE", OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME)
    VALUES
      (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    merge_sql = f"""
    MERGE INTO {target_table} t
    USING {stage} s
      ON  t.SYMBOL = s.SYMBOL
      AND t."DATE" = s."DATE"
    WHEN MATCHED THEN UPDATE SET
      t.OPEN      = s.OPEN,
      t.HIGH      = s.HIGH,
      t.LOW       = s.LOW,
      t.CLOSE     = s.CLOSE,
      t.VOLUME    = s.VOLUME,
      t.ADJ_CLOSE = s.ADJ_CLOSE,
      t.SOURCE    = 'yfinance',
      t.LOAD_TS   = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT
      (SYMBOL, "DATE", OPEN, HIGH, LOW, CLOSE, VOLUME, ADJ_CLOSE, SOURCE, LOAD_TS)
    VALUES
      (s.SYMBOL, s."DATE", s.OPEN, s.HIGH, s.LOW, s.CLOSE, s.VOLUME, s.ADJ_CLOSE, 'yfinance', CURRENT_TIMESTAMP());
    """

    cur = return_snowflake_conn()
    try:
        # create target and temp stage
        cur.execute(ddl_target)
        cur.execute("BEGIN")
        cur.execute(ddl_stage)

        # guard: if no rows extracted, abort WITHOUT touching target
        if not rows:
            cur.execute("ROLLBACK")
            return 0

        # insert into staging
        cur.executemany(insert_stage_sql, rows)

        # optional additional guard: ensure staging has rows
        cur.execute(f"SELECT COUNT(*) FROM {stage}")
        stage_count = cur.fetchone()[0]
        if stage_count == 0:
            cur.execute("ROLLBACK")
            raise ValueError("Staging table is empty; aborting load to maintain idempotency.")

        # upsert
        cur.execute(merge_sql)

        cur.execute("COMMIT")
        return stage_count
    except Exception:
        cur.execute("ROLLBACK")
        raise
    finally:
        try:
            cur.close()
        except Exception:
            pass

# ---------------------------
# DAG 
# ---------------------------
with DAG(
    dag_id='Lab2_StockPriceETL',
    start_date=datetime(2024, 9, 21),
    catchup=False,
    tags=['ETL'],
    schedule='30 2 * * *',           
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
) as dag:

    target_table = "RAW.TWO_STOCK_V2"

    raw_data = extract()
    tidy_rows = transform(raw_data)
    inserted = load(tidy_rows, target_table=target_table)

    # ---- Trigger ELT/dbt DAG ----
    trigger_elt = TriggerDagRunOperator(
        task_id="trigger_elt",
        trigger_dag_id="lab2_dbt_elt_pipeline",  # your ELT/dbt DAG's dag_id
        wait_for_completion=False,
        conf={
            "as_of_date": "{{ ds }}",
            "source_table": target_table,
            "symbols": "{{ var.value.stock_symbols }}",
            "lookback_days": "{{ var.value.lookback_days }}",
            "data_interval_start": "{{ data_interval_start.isoformat() }}",
            "data_interval_end": "{{ data_interval_end.isoformat() }}",
            "source_dag": "{{ dag.dag_id }}",
            "source_run_id": "{{ run_id }}",
        },
    )

    raw_data >> tidy_rows >> inserted >> trigger_elt
    