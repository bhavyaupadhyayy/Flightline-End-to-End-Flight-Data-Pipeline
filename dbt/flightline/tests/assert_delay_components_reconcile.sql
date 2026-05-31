-- When a flight is >= 15 min late, BTS delay-cause buckets should roughly add
-- up to the arrival delay. Allow 5 min of slack for rounding. Flags rows that
-- break the relationship, which usually means a bad source extract.
with f as (
    select
        flight_key,
        arr_delay_min,
        carrier_delay_min + weather_delay_min + nas_delay_min
            + security_delay_min + late_aircraft_delay_min as total_components
    from {{ ref('fct_flights') }}
    where is_on_time = false
)
select *
from f
where total_components > 0
  and abs(arr_delay_min - total_components) > 5
