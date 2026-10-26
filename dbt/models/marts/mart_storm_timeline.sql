-- One row per storm, airport and hour around the storm onset: the hourly mart aligned on a
-- common time axis, so storms and airports can be compared hour by hour. Built for BI tools
-- (Tableau) and the analysis notebook.
--
-- - onset: start of sustained winter precipitation (6+ consecutive winter hours), or the first
--   winter hour of the storm when there was no sustained period (IAH during Fern);
-- - window: from timeline_hours_before hours before the onset to timeline_hours_after hours after;
-- - storm_phase: before / precipitation / recovery / normal, from mart_storm_summary.
--   Hours of light precipitation before the sustained onset count as "before";
--   has_winter_precip still shows them.

{% set hours_before = var('timeline_hours_before') %}
{% set hours_after = var('timeline_hours_after') %}
{% set min_departures = 3 %}

with storms as (

    select
        summary.event_id,
        events.event_name,
        summary.airport_code,
        coalesce(summary.sustained_onset, summary.storm_onset)  as onset_hour,
        summary.sustained_onset is not null                     as has_sustained_onset,
        summary.precip_end,
        summary.recovery_h
    from {{ ref('mart_storm_summary') }} as summary
    inner join {{ ref('storm_events') }} as events
        on events.event_id = summary.event_id
    where summary.storm_onset is not null

),

aligned as (

    select
        hourly.airport_hour_id,
        storms.event_id,
        storms.event_name,
        hourly.airport_code,
        hourly.weather_hour,
        storms.onset_hour,
        storms.has_sustained_onset,
        datediff('hour', storms.onset_hour, hourly.weather_hour)   as hours_from_onset,
        storms.precip_end,
        storms.recovery_h,
        hourly.scheduled_departures,
        hourly.cancelled,
        hourly.delayed_15,
        hourly.cancellation_rate,
        hourly.has_winter_precip,
        hourly.weather_group,
        hourly.temp_c
    from {{ ref('mart_airport_hourly') }} as hourly
    inner join storms
        on  storms.event_id = hourly.event_id
        and storms.airport_code = hourly.airport_code
    where hourly.weather_hour >= storms.onset_hour - interval {{ hours_before }} hour
      and hourly.weather_hour <  storms.onset_hour + interval {{ hours_after }} hour

)

select
    airport_hour_id,
    event_id,
    event_name,
    airport_code,
    weather_hour,
    onset_hour,
    has_sustained_onset,
    hours_from_onset,
    -- 3-hour bins smooth out quiet night hours, as in the analysis notebook
    cast(floor(hours_from_onset / 3) * 3 as integer)                   as hours_from_onset_3h,
    case
        when weather_hour < onset_hour then 'before'
        when weather_hour <= precip_end then 'precipitation'
        -- no recovery within the data means the airport was still recovering
        when recovery_h is null
          or weather_hour < precip_end + to_hours(cast(recovery_h as bigint)) then 'recovery'
        else 'normal'
    end                                                                as storm_phase,

    -- departures
    scheduled_departures,
    cancelled,
    scheduled_departures - cancelled                                   as operated_departures,
    delayed_15,
    cancellation_rate,
    scheduled_departures >= {{ min_departures }}                       as is_busy_hour,

    -- weather
    has_winter_precip,
    weather_group,
    temp_c
from aligned