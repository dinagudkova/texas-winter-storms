-- One row per storm, airport and airline group: cancellations in the 24 hours before the
-- storm and during the precipitation, and the extra departure delay of the flights that
-- operated in winter hours. Same rules as section 7 of analysis/storm_comparison.ipynb:
--
-- - Airline groups come from the airline_groups seed; carriers not listed there are 'Other'.
-- - Onset: sustained_onset from mart_storm_summary, or storm_onset when there was no
--   sustained period. "before": onset - 24 h .. onset; "during": onset .. precip_end.
-- - Rates based on fewer than 30 departures are NULL.
-- - Extra delay: median departure delay of operated flights in winter hours minus the median
--   in hours without any weather phenomena, over the whole data window of the event.
--   NULL with fewer than 20 operated departures in winter hours.

{% set min_departures = 30 %}
{% set min_operated_winter = 20 %}


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
        coalesce(sustained_onset, storm_onset) as onset,
        precip_end
    from {{ ref('mart_storm_summary') }}
    where storm_onset is not null

),

windows as (

    select
        flights.event_id,
        flights.origin_airport as airport_code,
        flights.airline_group,
        count(*) filter (where flights.scheduled_dep_hour < bounds.onset)
            as departures_before,
        count(*) filter (where flights.scheduled_dep_hour < bounds.onset and flights.is_cancelled)
            as cancelled_before,
        count(*) filter (where flights.scheduled_dep_hour >= bounds.onset)
            as departures_during,
        count(*) filter (where flights.scheduled_dep_hour >= bounds.onset and flights.is_cancelled)
            as cancelled_during
    from flights
    inner join bounds
        on  bounds.event_id = flights.event_id
        and bounds.airport_code = flights.origin_airport
    where flights.scheduled_dep_hour >= bounds.onset - interval 24 hour
      and flights.scheduled_dep_hour <= bounds.precip_end
    group by all

),

delays as (

    select
        event_id,
        origin_airport as airport_code,
        airline_group,
        count(*) filter (where has_winter_precip)                     as operated_winter,
        median(dep_delay_min) filter (where has_winter_precip)        as median_delay_winter_min,
        median(dep_delay_min) filter (where weather_group = 'none')   as median_delay_normal_min
    from flights
    where not is_cancelled
      and (has_winter_precip or weather_group = 'none')
    group by all

),

keys as (

    select event_id, airport_code, airline_group from windows
    union
    select event_id, airport_code, airline_group from delays

),

combined as (

    select
        keys.event_id,
        keys.airport_code,
        keys.airline_group,
        coalesce(windows.departures_before, 0)  as departures_before,
        coalesce(windows.cancelled_before, 0)   as cancelled_before,
        coalesce(windows.departures_during, 0)  as departures_during,
        coalesce(windows.cancelled_during, 0)   as cancelled_during,
        coalesce(delays.operated_winter, 0)     as operated_winter,
        delays.median_delay_winter_min,
        delays.median_delay_normal_min
    from keys
    left join windows using (event_id, airport_code, airline_group)
    left join delays  using (event_id, airport_code, airline_group)

)

select
    md5(concat_ws('|', event_id, airport_code, airline_group)) as airline_storm_id,
    event_id,
    airport_code,
    airline_group,

    -- 24 hours before the onset
    departures_before,
    cancelled_before,
    case when departures_before >= {{ min_departures }}
        then round(cancelled_before / departures_before, 4)
    end as cancel_rate_before,

    -- onset to end of precipitation
    departures_during,
    cancelled_during,
    case when departures_during >= {{ min_departures }}
        then round(cancelled_during / departures_during, 4)
    end as cancel_rate_during,

    -- the group had enough departures during the precipitation in at least one storm
    -- at this airport, so it is worth showing next to the other storm
    max(departures_during) over (partition by airport_code, airline_group)
        >= {{ min_departures }} as is_comparable_group,

    -- operated flights in winter hours vs. hours without weather phenomena
    operated_winter,
    median_delay_winter_min,
    median_delay_normal_min,
    case when operated_winter >= {{ min_operated_winter }}
        then median_delay_winter_min - median_delay_normal_min
    end as extra_delay_min

from combined
