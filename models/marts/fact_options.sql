{{
  config(
    static_analysis = 'baseline',
    )
}}
with cte_all_rows as (
        select
            symbol,
            * exclude(symbol, id),
            modified_ts as ts
        from {{ ref("options") }}
    )
select unnest(arg_max(cte_all_rows, ts)) -- duckdb's arg_max not fully supported
from cte_all_rows
group by contractsymbol
