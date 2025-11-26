{% test unique_symbol_date(model) %}
with dups as (
  select symbol, date, count(*) as cnt
  from {{ model }}
  group by 1, 2
  having count(*) > 1
)
select * from dups
{% endtest %}
