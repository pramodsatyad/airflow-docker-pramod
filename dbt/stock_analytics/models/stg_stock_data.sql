SELECT
    symbol,
    date,
    open,
    high,
    low,
    close,
    volume
FROM {{ source('raw', 'two_stock_v2') }}
WHERE close IS NOT NULL
