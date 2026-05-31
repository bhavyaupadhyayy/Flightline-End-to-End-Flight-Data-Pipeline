with flights as (
    select * from {{ ref('stg_bts__ontime') }}
)

select
    flight_key,
    flight_date,
    flight_year,
    flight_month,
    carrier_code,
    flight_number,
    origin_airport,
    dest_airport,
    distance_mi,
    air_time_min,
    dep_delay_min,
    arr_delay_min,
    is_cancelled,
    is_diverted,
    cancellation_code,
    -- An "on-time" arrival in BTS terms is < 15 minutes late.
    case
        when is_cancelled or is_diverted then null
        when arr_delay_min < 15 then true
        else false
    end as is_on_time,
    coalesce(carrier_delay_min, 0)        as carrier_delay_min,
    coalesce(weather_delay_min, 0)        as weather_delay_min,
    coalesce(nas_delay_min, 0)            as nas_delay_min,
    coalesce(security_delay_min, 0)       as security_delay_min,
    coalesce(late_aircraft_delay_min, 0)  as late_aircraft_delay_min
from flights
