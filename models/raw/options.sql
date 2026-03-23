{{
    config(
        materialized="incremental",
        unique_key="id",
    )
}}

with options as (
    select
        *,
        'seed:option_history:' || snapshot_ts as filename
    from {{ ref("option_history") }}
)
select
    options.contractsymbol || '-' || options.filename as id,
    options.*,
    options.snapshot_ts as modified_ts,
    current_timestamp as updated_ts
from options
{% if is_incremental() %}
    where not exists (select 1 from {{ this }} ck where ck.filename = options.filename)
{% endif %}
