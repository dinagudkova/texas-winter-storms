-- One row per airport and day of the data windows: daily cancellations, whether the day falls
-- in the main storm period, and an explanation for unusual days.
--
-- - main storm period: from the day before the storm onset to the end of recovery (or three
--   days after the precipitation ended when no recovery was reached);
-- - unusual day: outside the storm period, 20+ scheduled departures and 5 %+ of them cancelled;
-- - explanation, in order: local snow or ice, local thunderstorms, otherwise weather elsewhere
--   in the network (airlines report weather, but there was none at the airport).

{% set min_departures = 20 %}
{% set min_cancel_rate = 0.05 %}

with daily as (

    select
        event_id,
        airport_code,
        cast(weather_hour as date)              as flight_day,
        sum(scheduled_departures)               as departures,
        sum(cancelled)                          as cancelled,
        sum(cancelled_weather)                  as cancelled_weather,
        sum(cancelled_carrier)                  as cancelled_carrier,
        sum(cancelled_nas)                      as cancelled_nas,
        sum(delayed_15)                         as delayed_15
    from {{ ref('mart_airport_hourly') }}
    group by all

),

weather as (

    select
        airport_code,
        cast(weather_hour as date)                      as flight_day,
        count(*) filter (where has_winter_precip)       as winter_hours,
        count(*) filter (where has_thunder)             as thunder_hours,
        max(wind_gust_max_kmh)                          as max_gust_kmh,
        min(visibility_min_km)                          as min_visibility_km
    from {{ ref('prep_weather_hourly') }}
    group by all

),

arrivals as (

    select
        dest_airport                                    as airport_code,
        cast(scheduled_arr_ts as date)                  as flight_day,
        count(*)                                        as arrivals,
        count(*) filter (where is_diverted)             as diverted_arrivals
    from {{ ref('prep_flights') }}
    where is_arrival
      and scheduled_arr_ts is not null
    group by all

),

storms as (

    select
        event_id,
        airport_code,
        cast(storm_onset as date) - 1                                                  as storm_first_day,
        cast(precip_end + to_hours(cast(coalesce(recovery_h, 72) as bigint)) as date)  as storm_last_day
    from {{ ref('mart_storm_summary') }}
    where storm_onset is not null

),

days as (

    select
        daily.*,
        storms.storm_first_day,
        storms.storm_last_day,
        coalesce(daily.flight_day between storms.storm_first_day and storms.storm_last_day, false) as in_storm,
        daily.cancelled / nullif(daily.departures, 0)                                   as cancel_rate,
        coalesce(weather.winter_hours, 0)                                               as winter_hours,
        coalesce(weather.thunder_hours, 0)                                              as thunder_hours,
        weather.max_gust_kmh,
        weather.min_visibility_km,
        coalesce(arrivals.arrivals, 0)                                                  as arrivals,
        coalesce(arrivals.diverted_arrivals, 0)                                         as diverted_arrivals
    from daily
    left join storms
        on  storms.event_id = daily.event_id
        and storms.airport_code = daily.airport_code
    left join weather
        on  weather.airport_code = daily.airport_code
        and weather.flight_day = daily.flight_day
    left join arrivals
        on  arrivals.airport_code = daily.airport_code
        and arrivals.flight_day = daily.flight_day

),

baseline as (

    -- a normal day: outside the storm period, no winter precipitation, enough departures
    select
        event_id,
        airport_code,
        median(cancel_rate)         as normal_cancel_rate,
        median(diverted_arrivals)   as normal_diverted_arrivals
    from days
    where not in_storm
      and winter_hours = 0
      and departures >= {{ min_departures }}
    group by all

),

flagged as (

    select
        days.*,
        baseline.normal_cancel_rate,
        baseline.normal_diverted_arrivals,
        not days.in_storm
            and days.departures >= {{ min_departures }}
            and days.cancel_rate >= {{ min_cancel_rate }}   as is_unusual
    from days
    left join baseline
        on  baseline.event_id = days.event_id
        and baseline.airport_code = days.airport_code

)

select
    airport_code || '_' || cast(flight_day as varchar)   as airport_day_id,
    *,
    case
        when not is_unusual then null
        when winter_hours > 0 then 'local snow or ice'
        when thunder_hours > 0 then 'local thunderstorms'
        else 'weather elsewhere in the network'
    end                                                  as unusual_explanation
from flagged