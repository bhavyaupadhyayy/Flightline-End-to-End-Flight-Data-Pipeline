with source as (
    select * from {{ source('raw', 'bts_ontime') }}
),

renamed as (
    select
        flightdate::date                              as flight_date,
        year                                          as flight_year,
        month                                         as flight_month,
        reporting_airline                             as carrier_code,
        flight_number_reporting_airline               as flight_number,
        origin                                        as origin_airport,
        origincityname                                as origin_city,
        dest                                          as dest_airport,
        destcityname                                  as dest_city,
        depdelay                                      as dep_delay_min,
        arrdelay                                      as arr_delay_min,
        coalesce(cancelled, 0) = 1                    as is_cancelled,
        coalesce(diverted, 0) = 1                     as is_diverted,
        cancellationcode                              as cancellation_code,
        airtime                                       as air_time_min,
        distance                                      as distance_mi,
        carrierdelay                                  as carrier_delay_min,
        weatherdelay                                  as weather_delay_min,
        nasdelay                                      as nas_delay_min,
        securitydelay                                 as security_delay_min,
        lateaircraftdelay                             as late_aircraft_delay_min,
        -- Natural key for a scheduled flight leg.
        {{ dbt_utils.generate_surrogate_key([
            'flightdate', 'reporting_airline',
            'flight_number_reporting_airline',
            'origin', 'dest', 'crsdeptime'
        ]) }}                                          as flight_key
    from source
),

deduped as (
    -- BTS occasionally double-publishes corrected rows; keep the latest load.
    select *,
        row_number() over (
            partition by flight_key order by flight_date
        ) as _rn
    from renamed
)

select * exclude (_rn)
from deduped
where _rn = 1
