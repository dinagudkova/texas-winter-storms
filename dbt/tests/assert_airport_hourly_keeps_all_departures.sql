-- Every departure must land in exactly one airport hour. If the hour grid misses some
-- departure hours, the totals differ and this returns a row.
with mart as (
    select sum(scheduled_departures) as departures from {{ ref('mart_airport_hourly') }}
),
source as (
    select count(*) as departures from {{ ref('prep_flights') }} where is_departure
)
select mart.departures as in_mart, source.departures as in_source
from mart, source
where mart.departures <> source.departures