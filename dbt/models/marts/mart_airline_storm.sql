-- One row per storm, airport and airline group: how each airline group handled the storm.
--
-- - cancellations in the 24 hours before the onset and from the onset to the end of winter
--   precipitation (onset = start of sustained precipitation, as in mart_storm_timeline);
-- - median departure delay and taxi-out of operated flights in winter hours compared with
--   normal hours (no weather phenomena) at the same airport in the data window.
-- Medians cannot be re-aggregated in BI tools, so they are computed here. Rates and extras
-- based on too few flights are left empty.

{% set min_departures = 30 %}
{% set min_operated = 20 %}

with flights as (

    select
        flights.*,
        coalesce(groups.airline_group, 'Other') as airline_group
    from {{ ref('mart_flights_weather') }} as flights
    left join {{ ref('airline_groups') }} as groups
        on groups.airline_code = flights.airline_code

),

bounds as (

    select
        event_id,
        airport_code,
        coalesce(sustained_onset, storm_onset) as onset_hour,
        precip_end
    from {{ ref('mart_storm_summary') }}
    where storm_onset is not null

),

window_counts as (

    select
        flights.event_id,
        flights.origin_airport                                                          as airport_code,
        flights.airline_group,
        count(*) filter (where flights.scheduled_dep_hour < bounds.onset_hour)          as departures_before,
        count(*) filter (where flights.scheduled_dep_hour < bounds.onset_hour
                           and flights.is_cancelled)                                    as cancelled_before,
        count(*) filter (where flights.scheduled_dep_hour >= bounds.onset_hour)         as departures_during,
        count(*) filter (where flights.scheduled_dep_hour >= bounds.onset_hour
                           and flights.is_cancelled)                                    as cancelled_during
    from flights
    inner join bounds
        on  bounds.event_id = flights.event_id
        and bounds.airport_code = flights.origin_airport
    where flights.scheduled_dep_hour >= bounds.onset_hour - interval 24 hour
      and flights.scheduled_dep_hour <= bounds.precip_end
    group by all

),

operated as (

    select
        event_id,
        origin_airport                                                       as airport_code,
        airline_group,
        count(*) filter (where has_winter_precip)                            as operated_winter,
        median(dep_delay_min) filter (where has_winter_precip)               as median_delay_winter_min,
        median(dep_delay_min) filter (where weather_group = 'none')          as median_delay_normal_min,
        median(taxi_out_min) filter (where has_winter_precip)                as median_taxi_out_winter_min,
        median(taxi_out_min) filter (where weather_group = 'none')           as median_taxi_out_normal_min
    from flights
    where not is_cancelled
      and (has_winter_precip or weather_group = 'none')
    group by all

),

keys as (

    select event_id, airport_code, airline_group from window_counts
    union
    select event_id, airport_code, airline_group from operated

)

select
    keys.event_id || '_' || keys.airport_code || '_' || keys.airline_group     as airline_storm_id,
    keys.event_id,
    keys.airport_code,
    keys.airline_group,

    -- cancellations around the onset
    coalesce(window_counts.departures_before, 0)                               as departures_before,
    coalesce(window_counts.cancelled_before, 0)                                as cancelled_before,
    coalesce(window_counts.departures_during, 0)                               as departures_during,
    coalesce(window_counts.cancelled_during, 0)                                as cancelled_during,
    case when window_counts.departures_before >= {{ min_departures }}
         then window_counts.cancelled_before / window_counts.departures_before end as cancel_rate_before,
    case when window_counts.departures_during >= {{ min_departures }}
         then window_counts.cancelled_during / window_counts.departures_during end as cancel_rate_during,

    -- operated flights in winter hours vs normal hours
    coalesce(operated.operated_winter, 0)                                      as operated_winter,
    operated.median_delay_winter_min,
    operated.median_delay_normal_min,
    case when operated.operated_winter >= {{ min_operated }}
         then operated.median_delay_winter_min - operated.median_delay_normal_min end   as extra_delay_min,
    case when operated.operated_winter >= {{ min_operated }}
         then operated.median_taxi_out_winter_min - operated.median_taxi_out_normal_min end as extra_taxi_out_min
from keys
left join window_counts
    on  window_counts.event_id = keys.event_id
    and window_counts.airport_code = keys.airport_code
    and window_counts.airline_group = keys.airline_group
left join operated
    on  operated.event_id = keys.event_id
    and operated.airport_code = keys.airport_code
    and operated.airline_group = keys.airline_group