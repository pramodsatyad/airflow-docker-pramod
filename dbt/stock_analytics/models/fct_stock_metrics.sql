with base as (
    select
        symbol,
        date,
        close,
        -- daily change
        close - lag(close) over (
            partition by symbol
            order by date
        ) as price_change
    from {{ ref('stg_stock_data') }}
),
gains as (
    select
        symbol,
        date,
        close,
        price_change,
        case when price_change > 0 then price_change else 0 end as gain,
        case when price_change < 0 then abs(price_change) else 0 end as loss
    from base
),
averages as (
    select
        symbol,
        date,
        close,
        price_change,
        gain,
        loss,
        avg(gain) over (
            partition by symbol
            order by date
            rows between 13 preceding and current row
        ) as avg_gain_14,

        avg(loss) over (
            partition by symbol
            order by date
            rows between 13 preceding and current row
        ) as avg_loss_14
    from gains
),

final as (
    select
        symbol,
        date,
        close,
        -- moving averages you already had
        round(
            avg(close) over (
                partition by symbol
                order by date
                rows between 6 preceding and current row
            ),
        2) as moving_avg_7d,
        round(
            avg(close) over (
                partition by symbol
                order by date
                rows between 29 preceding and current row
            ),
        2) as moving_avg_30d,
        -- percent change
        (price_change / lag(close) over (
            partition by symbol
            order by date
        )) as pct_change,
        -- RSI 
        case 
            when avg_loss_14 = 0 then 100
            else 100 - (100 / (1 + (avg_gain_14 / avg_loss_14)))
        end as rsi_14
    from averages
)
select *
from final
order by symbol, date
