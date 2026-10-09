-- One row per airport and local hour: scheduled departures and their outcome
-- next to the weather observed in that hour.
--
-- The grid comes from prep_weather_hourly, so hours without departures (night hours,
-- or hours when every flight had been removed from the schedule) are kept with zero counts.
-- Rates are NULL when no departures were scheduled; filter on scheduled_departures
-- before reading rates for quiet hours, where one cancellation can mean 100 %.


with weather as (

    select * from {{ ref('prep_weather_hourly') }}

),

departures as (

    select
        origin_airport                                                  as airport_code,
        scheduled_dep_hour                                              as weather_hour,
        count(*)                                                        as scheduled_departures,
        count(*) filter (where is_cancelled)                            as cancelled,
        count(*) filter (where cancellation_reason = 'weather')         as cancelled_weather,
        count(*) filter (where cancellation_reason = 'carrier')         as cancelled_carrier,
        count(*) filter (where cancellation_reason = 'nas')             as cancelled_nas,
        count(*) filter (where cancellation_reason = 'security')        as cancelled_security,
        count(*) filter (where is_diverted)                             as diverted,
        count(*) filter (where is_dep_delayed_15)                       as delayed_15,
        avg(dep_delay_min) filter (where not is_cancelled)              as avg_dep_delay_min,
        avg(taxi_out_min) filter (where not is_cancelled)               as avg_taxi_out_min
    from {{ ref('prep_flights') }}
    where is_departure
    group by all

),

events as (

    select * from {{ ref('storm_events') }}

)

select
    weather.weather_hour_id                                             as airport_hour_id,
    weather.airport_code,
    weather.weather_hour,
    weather.weather_date,

-- storm event and period
events.event_id,
case
    when weather.weather_hour < events.storm_start then 'before'
    when weather.weather_hour < events.storm_end + interval 1 day then 'storm'
    else 'after'
end as storm_period,
datediff(
    'hour',
    cast(
        events.storm_start as timestamp
    ),
    weather.weather_hour
) as hours_from_storm_start,

-- departures
coalesce(
    departures.scheduled_departures,
    0
) as scheduled_departures,
coalesce(departures.cancelled, 0) as cancelled,
coalesce(
    departures.cancelled_weather,
    0
) as cancelled_weather,
coalesce(
    departures.cancelled_carrier,
    0
) as cancelled_carrier,
coalesce(departures.cancelled_nas, 0) as cancelled_nas,
coalesce(
    departures.cancelled_security,
    0
) as cancelled_security,
coalesce(departures.diverted, 0) as diverted,
coalesce(departures.delayed_15, 0) as delayed_15,
round(
    departures.cancelled / nullif(
        departures.scheduled_departures,
        0
    ),
    4
) as cancellation_rate,
round(
    departures.delayed_15 / nullif(
        departures.scheduled_departures - departures.cancelled,
        0
    ),
    4
) as delay_15_rate,
round(
    departures.avg_dep_delay_min,
    1
) as avg_dep_delay_min,
round(
    departures.avg_taxi_out_min,
    1
) as avg_taxi_out_min,

-- weather in the same hour
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
    weather
    left join departures on departures.airport_code = weather.airport_code
    and departures.weather_hour = weather.weather_hour
    left join events on weather.weather_date between events.data_start and events.data_end