{{
    config(
        materialized="incremental",
        unique_key="id",
    )
}}

{% set my_list = run_query("select file from " ~ ref('files') ~ " where entity = 'option_history'").columns[0].values() %}

select
    options.contractsymbol || '-' || options.filename as id,
    options.*,
    files.modified_ts,
    now() at time zone 'UTC' as updated_ts
from read_csv([{% for f in my_list %}'{{ f }}'{% if not loop.last %}, {% endif %}{% endfor %}], filename = true, union_by_name = true) as options
left join {{ ref("files") }} as files on options.filename = files.file
{% if is_incremental() %}
    where not exists (select 1 from {{ this }} ck where ck.filename = options.filename)
{% endif %}
