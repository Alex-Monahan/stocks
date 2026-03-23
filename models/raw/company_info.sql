{{
    config(
        materialized="incremental",
        unique_key="id",
    )
}}

{% set my_list = run_query("select file from " ~ ref('files') ~ " where entity = 'ticker_info'").columns[0].values() %}

select
    info.symbol || '-' || info.filename as id,
    info.*,
    files.modified_ts,
    now() at time zone 'UTC' as updated_ts
from read_csv([{% for f in my_list %}'{{ f }}'{% if not loop.last %}, {% endif %}{% endfor %}], filename = true, union_by_name = true) as info
left join {{ ref("files") }} as files on info.filename = files.file
{% if is_incremental() %}
    where not exists (select 1 from {{ this }} ck where ck.filename = info.filename)
{% endif %}
