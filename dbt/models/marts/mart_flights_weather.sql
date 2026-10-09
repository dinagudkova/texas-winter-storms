-- One row per flight departing from DFW, IAH, AUS or SAT, with the weather at the origin
-- airport in the hour of the scheduled departure and the storm period it falls into.
--
-- Scheduled (not actual) departure is used for the weather join, because cancelled
-- flights have no actual departure, and cancellations are the main outcome studied.


with departures as (

    select * from {{ ref('prep_flights') }}
    where is_departure

),

weather as (

    select * from {{ ref('prep_weather_hourly') }}

),

events as (

    select * from {{ ref('storm_events') }}

)

select
    -- flight
    departures.flight_id,
    departures.flight_date,
    departures.airline_code,
    departures.flight_number,
    departures.origin_airport,
    departures.dest_airport,
    departures.scheduled_dep_ts,
    departures.scheduled_dep_hour,

-- storm event and period
events.event_id,
events.event_name,
case
    when departures.scheduled_dep_ts < events.storm_start then 'before'
    when departures.scheduled_dep_ts < events.storm_end + interval 1 day then 'storm'
    else 'after'
end as storm_period,
-- hours from the start of the storm window, negative before it
datediff(
    'hour',
    cast(
        events.storm_start as timestamp
    ),
    departures.scheduled_dep_hour
) as hours_from_storm_start,

-- outcome
departures.is_cancelled,
departures.cancellation_code,
departures.cancellation_reason,
departures.is_diverted,
departures.dep_delay_min,
departures.is_dep_delayed_15,
departures.taxi_out_min,
departures.weather_delay_min,
departures.carrier_delay_min,
departures.nas_delay_min,
departures.late_aircraft_delay_min,

-- weather at the origin airport in the hour of the scheduled departure
weather.weather_hour_id,
weather.has_report as has_weather_report,
weather.weather_group,
weather.has_winter_precip,
weather.has_freezing_rain,
weather.has_ice_pellets,
weather.has_snow,
weather.temp_c,
weather.temp_min_c,
weather.precip_mm,
weather.ice_accretion_max_mm,
weather.visibility_min_km,
weather.wind_gust_max_kmh
from
    departures
    left join weather on weather.airport_code = departures.origin_airport
    and weather.weather_hour = departures.scheduled_dep_hour
    left join events on departures.flight_date between events.data_start and events.data_end