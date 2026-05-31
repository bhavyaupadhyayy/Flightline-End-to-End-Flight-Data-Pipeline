with flights as (
    select distinct carrier_code from {{ ref('stg_bts__ontime') }}
),

lookup as (
    select * from {{ ref('seed_carrier_lookup') }}
)

select
    f.carrier_code,
    coalesce(l.carrier_name, f.carrier_code) as carrier_name
from flights f
left join lookup l on f.carrier_code = l.carrier_code
