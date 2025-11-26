{% snapshot stock_prices_snapshot %}
{{ config(
    target_schema='SNAPSHOTS',          
    unique_key='symbol_date_key',       
    strategy='check',                   
    check_cols=['open','high','low','close','volume']
) }}
select
  symbol,
  date,
  open,
  high,
  low,
  close,
  volume,
  concat(symbol, '-', to_char(date, 'YYYY-MM-DD')) as symbol_date_key
from {{ ref('stg_stock_data') }}
{% endsnapshot %}