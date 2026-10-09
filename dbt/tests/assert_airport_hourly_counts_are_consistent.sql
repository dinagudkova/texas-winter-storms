-- Hours where the counts contradict each other. The test passes when this returns no rows.
select *
from {{ ref('mart_airport_hourly') }}
where cancelled > scheduled_departures
   or cancelled_weather + cancelled_carrier + cancelled_nas + cancelled_security <> cancelled
   or delayed_15 > scheduled_departures - cancelled