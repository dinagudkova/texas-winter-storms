-- One row per storm event and airport: when winter precipitation started and ended,
-- how quickly cancellations followed and how long recovery took.
--
-- Rules (the same for every storm and airport):
-- - Search window: storm_start - 3 days .. storm_end + 3 days from the storm_events seed.
--   The seed only gives approximate dates; exact boundaries come from the weather.
-- - Winter episode: hours with winter precipitation (freezing rain/drizzle, ice pellets, snow);
--   a gap of 24+ hours without winter precipitation starts a new episode.
-- - Main episode: the episode with the most winter hours (the earliest one on a tie).
--   Smaller episodes near the storm, like the ice on 11 February 2021 before Uri,
--   are counted in n_winter_episodes but do not define the storm.
-- - Storm onset / precipitation end: first / last winter hour of the main episode.
-- - Sustained onset: first hour of the main episode that starts 6+ consecutive winter hours.
--   Light, intermittent icing before it (common before Uri) is ignored by this second onset.
-- - Mass cancellation: first hour, from 24 hours before onset, with 3+ scheduled departures
--   and a cancellation rate of 50 %+. The lag is negative when cancellations started before
--   the precipitation.
-- - Recovery: first hour after the precipitation end that starts 6 consecutive hours
--   (counting only hours with 3+ scheduled departures) with a cancellation rate below 10 %.

{% set min_departures = 3 %}


with events as (

    select
        *,
        cast(storm_start as timestamp) - interval 3 day                       as search_start,
        cast(storm_end as timestamp) + interval 1 day + interval 3 day        as search_end
    from {{ ref('storm_events') }}

),

hours as (

    select hourly.*
    from {{ ref('mart_airport_hourly') }} as hourly
    inner join events
        on  hourly.event_id = events.event_id
        and hourly.weather_hour >= events.search_start
        and hourly.weather_hour <  events.search_end

),

-- all hours of the event, for totals and recovery that may reach outside the search window

all_hours as (

    select * from {{ ref('mart_airport_hourly') }}

),

-- every event and airport gets a row, even without winter precipitation in the search window
pairs as (
    select distinct
        event_id,
        airport_code
    from hours
),

-- split winter hours into episodes separated by 24+ hours without winter precipitation
winter_hours as (
    select
        event_id,
        airport_code,
        weather_hour,
        datediff(
            'hour',
            lag(weather_hour) over (
                partition by
                    event_id,
                    airport_code
                order by weather_hour
            ),
            weather_hour
        ) as hours_since_previous
    from hours
    where
        has_winter_precip
),
episodes as (
    select *, sum(
            case
                when hours_since_previous is null
                or hours_since_previous >= 24 then 1
                else 0
            end
        ) over (
            partition by
                event_id, airport_code
            order by weather_hour
        ) as episode_number
    from winter_hours
),
episode_sizes as (
    select
        event_id,
        airport_code,
        episode_number,
        min(weather_hour) as episode_start,
        max(weather_hour) as episode_end,
        count(*) as episode_hours
    from episodes
    group by
        all
),
storm_bounds as (
    select
        event_id,
        airport_code,
        arg_max (
            episode_start,
            (
                episode_hours,
                - episode_number
            )
        ) as storm_onset,
        arg_max (
            episode_end,
            (
                episode_hours,
                - episode_number
            )
        ) as precip_end,
        count(*) as n_winter_episodes
    from episode_sizes
    group by
        all
),

-- runs of consecutive winter hours (no gap at all), to find when precipitation became sustained

runs as (

    select
        event_id,
        airport_code,
        run_number,
        min(weather_hour)   as run_start,
        count(*)            as run_hours
    from (
        select
            *,
            sum(case when hours_since_previous is null or hours_since_previous > 1 then 1 else 0 end)
                over (partition by event_id, airport_code order by weather_hour) as run_number
        from winter_hours
    ) as numbered
    group by all

),

sustained as (

    select
        runs.event_id,
        runs.airport_code,
        min(runs.run_start) as sustained_onset
    from runs
    inner join storm_bounds
        on  storm_bounds.event_id = runs.event_id
        and storm_bounds.airport_code = runs.airport_code
    where runs.run_hours >= 6
      and runs.run_start between storm_bounds.storm_onset and storm_bounds.precip_end
    group by all

),

mass_cancellation as (

    select
        hours.event_id,
        hours.airport_code,
        min(hours.weather_hour) as first_mass_cancel_hour
    from hours
    inner join storm_bounds
        on  storm_bounds.event_id = hours.event_id
        and storm_bounds.airport_code = hours.airport_code
    -- look from 24 hours before onset, so early cancellations show up as a negative lag
    -- but disruption from an earlier, unrelated episode does not
    where hours.weather_hour >= storm_bounds.storm_onset - interval 24 hour
      and hours.scheduled_departures >= {{ min_departures }}
      and hours.cancellation_rate >= 0.5
    group by all

),

-- busy hours after the precipitation end, with a flag for normal operations

after_precip as (

    select
        hours.event_id,
        hours.airport_code,
        hours.weather_hour,
        hours.cancellation_rate < 0.1 as is_normal
    from all_hours as hours
    inner join storm_bounds
        on  storm_bounds.event_id = hours.event_id
        and storm_bounds.airport_code = hours.airport_code
    where hours.weather_hour > storm_bounds.precip_end
      and hours.scheduled_departures >= {{ min_departures }}

),

recovery_candidates as (

    select
        *,
        bool_and(is_normal) over six_hours  as next_six_normal,
        count(*) over six_hours             as hours_in_frame
    from after_precip
    window six_hours as (
        partition by event_id, airport_code
        order by weather_hour
        rows between current row and 5 following
    )

),

recovery as (

    select event_id, airport_code, min(weather_hour) as recovery_hour
    from recovery_candidates
    where next_six_normal and hours_in_frame = 6
    group by all

),

bounds as (

    select
        storm_bounds.*,
        sustained.sustained_onset,
        mass_cancellation.first_mass_cancel_hour,
        recovery.recovery_hour
    from storm_bounds
    left join sustained using (event_id, airport_code)
    left join mass_cancellation using (event_id, airport_code)
    left join recovery using (event_id, airport_code)

),

-- weather and flight totals inside the storm and around it

totals as (

    select
        bounds.event_id,
        bounds.airport_code,

-- weather during the main winter episode
count(*) filter (
    where
        hours.weather_hour between bounds.storm_onset and bounds.precip_end
        and hours.has_winter_precip
) as winter_precip_hours,
count(*) filter (
    where
        hours.weather_hour between bounds.storm_onset and bounds.precip_end
        and hours.has_freezing_rain
) as freezing_rain_hours,
count(*) filter (
    where
        hours.weather_hour between bounds.storm_onset and bounds.precip_end
        and hours.has_ice_pellets
) as ice_pellets_hours,
count(*) filter (
    where
        hours.weather_hour between bounds.storm_onset and bounds.precip_end
        and hours.has_snow
) as snow_hours,
min(hours.temp_min_c) as min_temp_c,
max(hours.ice_accretion_max_mm) as max_ice_accretion_mm,

-- the 24 hours before onset: were flights cancelled in advance?
sum(hours.scheduled_departures) filter (
    where
        hours.weather_hour >= bounds.storm_onset - interval 24 hour
        and hours.weather_hour < bounds.storm_onset
) as pre_onset_departures,
sum(hours.cancelled) filter (
    where
        hours.weather_hour >= bounds.storm_onset - interval 24 hour
        and hours.weather_hour < bounds.storm_onset
) as pre_onset_cancelled,

-- disruption window: onset until recovery (or the precipitation end if no recovery found)
sum(hours.scheduled_departures) filter (
            where hours.weather_hour >= bounds.storm_onset
              and hours.weather_hour <  coalesce(bounds.recovery_hour, bounds.precip_end + interval 1 hour))
                                                                                as disruption_departures,
        sum(hours.cancelled) filter (
            where hours.weather_hour >= bounds.storm_onset
              and hours.weather_hour <  coalesce(bounds.recovery_hour, bounds.precip_end + interval 1 hour))
                                                                                as disruption_cancelled,
        sum(hours.cancelled_weather) filter (
            where hours.weather_hour >= bounds.storm_onset
              and hours.weather_hour <  coalesce(bounds.recovery_hour, bounds.precip_end + interval 1 hour))
                                                                                as disruption_cancelled_weather

    from bounds
    inner join all_hours as hours
        on  hours.event_id = bounds.event_id
        and hours.airport_code = bounds.airport_code
    group by all

)

select md5(
        concat_ws(
            '|', pairs.event_id, pairs.airport_code
        )
    ) as storm_airport_id, pairs.event_id, pairs.airport_code,

-- storm timing from the weather
bounds.storm_onset,
bounds.precip_end,
datediff(
    'hour',
    bounds.storm_onset,
    bounds.precip_end
) + 1 as storm_duration_h,
bounds.n_winter_episodes,
totals.winter_precip_hours,
totals.freezing_rain_hours,
totals.ice_pellets_hours,
totals.snow_hours,
totals.min_temp_c,
totals.max_ice_accretion_mm,

-- response of flight operations
bounds.sustained_onset,
bounds.first_mass_cancel_hour,
datediff(
    'hour',
    bounds.storm_onset,
    bounds.first_mass_cancel_hour
) as lag_to_mass_cancel_h,
datediff(
    'hour',
    bounds.sustained_onset,
    bounds.first_mass_cancel_hour
) as lag_from_sustained_h,
bounds.recovery_hour,
datediff(
    'hour',
    bounds.precip_end,
    bounds.recovery_hour
) as recovery_h,
totals.pre_onset_departures,
totals.pre_onset_cancelled,
round(
    totals.pre_onset_cancelled / nullif(
        totals.pre_onset_departures,
        0
    ),
    4
) as pre_onset_cancel_rate,
totals.disruption_departures,
totals.disruption_cancelled,
round(
    totals.disruption_cancelled / nullif(
        totals.disruption_departures,
        0
    ),
    4
) as disruption_cancel_rate,
round(
    totals.disruption_cancelled_weather / nullif(
        totals.disruption_cancelled,
        0
    ),
    4
) as weather_reason_share
from pairs
    left join bounds using (event_id, airport_code)
    left join totals using (event_id, airport_code)