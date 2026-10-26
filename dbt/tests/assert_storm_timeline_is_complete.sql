-- Every storm and airport must have the full window of consecutive hours around the onset.
-- The test passes when this returns no rows.
{% set hours_before = var('timeline_hours_before') %}
{% set hours_after = var('timeline_hours_after') %}

select
    event_id,
    airport_code,
    count(*)                as hours,
    min(hours_from_onset)   as first_hour,
    max(hours_from_onset)   as last_hour
from {{ ref('mart_storm_timeline') }}
group by all
having count(*) <> {{ hours_before + hours_after }}
    or count(distinct hours_from_onset) <> count(*)
    or min(hours_from_onset) <> -{{ hours_before }}
    or max(hours_from_onset) <> {{ hours_after - 1 }}