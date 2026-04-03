{{
    config(
        materialized="incremental",
        unique_key="id",
    )
}}

with info as (
    select
        *,
        'seed:ticker_info:' || snapshot_ts as filename
    from {{ ref("ticker_info") }}
)
select
    info.symbol || '-' || info.filename as id,
    info.*,
    info.snapshot_ts as modified_ts,
    current_timestamp as updated_ts
from info
{% if is_incremental() %}
    where not exists (select 1 from {{ this }} ck where ck.filename = info.filename)
{% endif %}
