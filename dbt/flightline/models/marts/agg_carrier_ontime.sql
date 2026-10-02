with f as (
    select * from {{ ref('fct_flights') }}
)

select
    c.carrier_code,
    c.carrier_name,
    count(*)                                                as total_flights,
    sum(case when f.is_cancelled then 1 else 0 end)         as cancelled_flights,
    avg(f.arr_delay_min)                                    as avg_arr_delay_min,
    -- Share of completed flights that arrived on time; null when none completed.
    count_if(f.is_on_time) / nullif(count(f.is_on_time), 0) as on_time_rate
from f
inner join {{ ref('dim_carrier') }} as c on f.carrier_code = c.carrier_code
group by c.carrier_code, c.carrier_name
order by on_time_rate desc nulls last
