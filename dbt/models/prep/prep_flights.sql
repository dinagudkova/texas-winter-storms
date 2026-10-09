-- One row per scheduled flight, with local timestamps and the hours used to join weather.
--
-- Times in BTS are local hhmm text without a date. Timestamps are built as follows:
-- - scheduled departure: flight_date + scheduled time (origin local time);
-- - actual departure: scheduled departure + departure delay, which handles flights
--   that leave after midnight and early departures without guessing the date;
-- - scheduled arrival: only for arrivals at the project airports (US Central time),
--   moved to the next day when it would otherwise be far before the departure.

{% set airports = var('airports') %}


with flights as (

    select * from {{ ref('stg_flights') }}

),

timed as (

    select
        *,
        flight_date + to_minutes({{ hhmm_to_minutes('scheduled_dep_hhmm') }}) as scheduled_dep_ts,
        flight_date + to_minutes({{ hhmm_to_minutes('scheduled_arr_hhmm') }}) as scheduled_arr_candidate
    from flights

)

select
    flight_id,
    flight_date,
    airline_code,
    flight_number,
    tail_number,
    origin_airport,
    dest_airport,

-- direction relative to the project airports; a flight between two of them is both
origin_airport in (
    '{{ airports | join("',
    '") }}'
) as is_departure,
dest_airport in (
    '{{ airports | join("',
    '") }}'
) as is_arrival,

-- departure, origin local time
scheduled_dep_ts,
date_trunc ('hour', scheduled_dep_ts) as scheduled_dep_hour,
case
    when not is_cancelled
    and dep_delay_min is not null then scheduled_dep_ts + to_minutes (
        cast(dep_delay_min as integer)
    )
end as actual_dep_ts,

-- arrival, Central time; only meaningful when the destination is a project airport.
-- Origins in other time zones shift clock times by up to a few hours, so an arrival
-- more than 6 hours "before" departure means it lands on the next day.
case
    when dest_airport in (
        '{{ airports | join("',
        '") }}'
    ) then case
        when scheduled_arr_candidate < scheduled_dep_ts - interval 6 hour then scheduled_arr_candidate + interval 1 day
        else scheduled_arr_candidate
    end
end as scheduled_arr_ts,

-- outcome
is_cancelled,
cancellation_code,
cancellation_reason,
is_diverted,
dep_delay_min,
arr_delay_min,
-- delayed = operated and left 15+ minutes late; some cancelled flights also carry a
-- departure delay (they left the gate, e.g. for de-icing, and were cancelled afterwards)
not is_cancelled
and dep_delay_min >= 15 as is_dep_delayed_15,
taxi_out_min,

-- delay causes, minutes
carrier_delay_min,
weather_delay_min,
nas_delay_min,
security_delay_min,
late_aircraft_delay_min,
distance_km
from timed