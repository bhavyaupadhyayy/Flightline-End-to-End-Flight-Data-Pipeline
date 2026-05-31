-- A flight cannot have negative distance. If this returns rows, the test fails.
select flight_key, distance_mi
from {{ ref('fct_flights') }}
where distance_mi < 0
