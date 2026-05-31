with f as (
    select * from {{ ref('fct_flights') }}
)

select
    c.carrier_code,
    c.carrier_name,
    count(*)                                              as total_flights,
    sum(case when f.is_cancelled then 1 else 0 end)       as cancelled_flights,
    avg(f.arr_delay_min)                                  as avg_arr_delay_min,
    sum(case when f.is_on_time then 1 else 0 end)
        / nullif(sum(case when f.is_on_time is not null then 1 else 0 end), 0)
                                                          as on_time_rate
from f
join {{ ref('dim_carrier') }} c on f.carrier_code = c.carrier_code
group by 1, 2
order by on_time_rate desc nulls last
