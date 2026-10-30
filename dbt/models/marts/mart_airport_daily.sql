-- One row per storm event, airport and local day of the data window: departures,
-- cancellations, the weather of the day and diverted arrivals, with a flag for unusual
-- days outside the main storm. Same rules as section 8 of analysis/storm_comparison.ipynb:
--
-- - Main storm period: from the day before the storm onset to the end of recovery
--   (precip_end + recovery_h), or three days after the precipitation ended when no
--   recovery was reached.
-- - Unusual day: outside the main storm period, 20+ scheduled departures and 5 %+ of them
--   cancelled (a normal day has 0.2-0.3 %).
-- - Explanation of an unusual day, in this order: winter precipitation at the airport
--   ('local snow or ice'), thunder at the airport ('thunderstorms'), otherwise
--   'weather elsewhere' (airlines still coded the cancellations as weather: aircraft and
--   crews were held up by storms in other parts of the network).
-- - Diverted arrivals come from prep_flights, the only model that keeps arrivals.

{% set min_day_departures = 20 %}
{% set cancel_threshold = 0.05 %}
{% set recovery_fallback_h = 72 %}


with departures as (

    select
        event_id,
        airport_code,
        weather_date                as day,
        sum(scheduled_departures)   as departures,
        sum(cancelled)              as cancelled,
        sum(cancelled_weather)      as cancelled_weather,
        sum(cancelled_carrier)      as cancelled_carrier,
        sum(cancelled_nas)          as cancelled_nas,
        sum(delayed_15)             as delayed_15
    from {{ ref('mart_airport_hourly') }}
    where event_id is not null
    group by all

),

weather as (

    select
        airport_code,
        weather_date                                     as day,
        count(*) filter (where has_winter_precip)        as winter_hours,
        count(*) filter (where has_thunder)              as thunder_hours,
        count(*) filter (where has_rain)                 as rain_hours,
        min(temp_c)                                      as min_temp_c,
        max(wind_gust_max_kmh)                           as max_gust_kmh,
        min(visibility_min_km)                           as min_visibility_km
    from {{ ref('prep_weather_hourly') }}
    group by all

),

arrivals as (

    select
        dest_airport                              as airport_code,
        cast(scheduled_arr_ts as date)            as day,
        count(*)                                  as arrivals,
        count(*) filter (where is_diverted)       as diverted_arrivals
    from {{ ref('prep_flights') }}
    where is_arrival
      and scheduled_arr_ts is not null
    group by all

),

normal_diversions as (

    select
        airport_code,
        year(day)                       as year,
        median(diverted_arrivals)       as normal_diverted_arrivals
    from arrivals
    group by all

),

storm_days as (

    select
        event_id,
        airport_code,
        cast(storm_onset - interval 1 day as date)                                   as storm_first_day,
        cast(precip_end + to_hours(coalesce(recovery_h, {{ recovery_fallback_h }})) as date)
                                                                                     as storm_last_day
    from {{ ref('mart_storm_summary') }}
    where storm_onset is not null

),

days as (

    select
        departures.*,
        events.event_name,
        storm_days.storm_first_day,
        storm_days.storm_last_day,
        coalesce(departures.day between storm_days.storm_first_day and storm_days.storm_last_day, false)
            as is_storm_period,
        round(departures.cancelled / nullif(departures.departures, 0), 4)  as cancel_rate,
        coalesce(weather.winter_hours, 0)                                  as winter_hours,
        coalesce(weather.thunder_hours, 0)                                 as thunder_hours,
        coalesce(weather.rain_hours, 0)                                    as rain_hours,
        weather.min_temp_c,
        weather.max_gust_kmh,
        weather.min_visibility_km,
        coalesce(arrivals.arrivals, 0)                                     as arrivals,
        coalesce(arrivals.diverted_arrivals, 0)                            as diverted_arrivals,
        normal_diversions.normal_diverted_arrivals
    from departures
    left join {{ ref('storm_events') }} as events
        on events.event_id = departures.event_id
    left join storm_days
        on  storm_days.event_id = departures.event_id
        and storm_days.airport_code = departures.airport_code
    left join weather
        on  weather.airport_code = departures.airport_code
        and weather.day = departures.day
    left join arrivals
        on  arrivals.airport_code = departures.airport_code
        and arrivals.day = departures.day
    left join normal_diversions
        on  normal_diversions.airport_code = departures.airport_code
        and normal_diversions.year = year(departures.day)

),

flagged as (

    select
        *,
        not is_storm_period
            and departures >= {{ min_day_departures }}
            and cancel_rate >= {{ cancel_threshold }}   as is_unusual
    from days

)

select
    md5(concat_ws('|', event_id, airport_code, day)) as airport_day_id,
    event_id,
    event_name,
    airport_code,
    day,
    -- the same calendar day in 2021, so both data windows share one date axis in BI tools
    make_date(2021, month(day), dayofmonth(day))            as aligned_day,

    -- departures
    departures,
    cancelled,
    cancel_rate,
    cancelled_weather,
    cancelled_carrier,
    cancelled_nas,
    delayed_15,
    case when cancelled >= 5 then
        case greatest(cancelled_weather, cancelled_carrier, cancelled_nas)
            when cancelled_weather then 'weather'
            when cancelled_carrier then 'carrier'
            else 'nas'
        end
    end                                              as main_cancel_reason,

    -- storm period and unusual days
    storm_first_day,
    storm_last_day,
    is_storm_period,
    coalesce(is_unusual, false)                      as is_unusual,
    case when is_unusual then
        case
            when winter_hours > 0  then 'local snow or ice'
            when thunder_hours > 0 then 'thunderstorms'
            else 'weather elsewhere'
        end
    end                                              as unusual_reason,

    -- weather of the day at the airport
    winter_hours,
    thunder_hours,
    rain_hours,
    min_temp_c,
    max_gust_kmh,
    min_visibility_km,

    -- arrivals diverted away from the airport
    arrivals,
    diverted_arrivals,
    normal_diverted_arrivals

from flagged
