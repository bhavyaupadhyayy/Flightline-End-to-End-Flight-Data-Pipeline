select
    origin_airport,
    dest_airport,
    count(*)              as total_flights,
    avg(arr_delay_min)    as avg_arr_delay_min,
    avg(dep_delay_min)    as avg_dep_delay_min,
    avg(distance_mi)      as distance_mi
from {{ ref('fct_flights') }}
group by 1, 2
having count(*) >= 30
order by avg_arr_delay_min desc
