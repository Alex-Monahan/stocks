{{
    config(
        materialized="incremental",
        unique_key="id",
    )
}}

with stocks as (
    select
        *,
        'seed:ticker_history:' || snapshot_ts as filename
    from {{ ref("ticker_history") }}
)
select
    symbol || '-' || date || '-' || filename as id,
    stocks.*,
    stocks.snapshot_ts as modified_ts,
    current_timestamp as updated_ts
from stocks
{% if is_incremental() %}
    where not exists (select 1 from {{ this }} ck where ck.filename = stocks.filename)
{% endif %}
