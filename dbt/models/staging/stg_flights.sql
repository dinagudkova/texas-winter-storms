-- One row per scheduled flight from or to the four airports (BTS On-Time Performance).
-- Staging only renames, types and decodes; time parsing happens in prep.


with source as (

    select * from {{ source('raw', 'flights') }}

),

renamed as (

    select
        -- identifiers
        "FlightDate"                      as flight_date,
        "Reporting_Airline"               as airline_code,
        "Flight_Number_Reporting_Airline" as flight_number,
        "Tail_Number"                     as tail_number,
        "Origin"                          as origin_airport,
        "Dest"                            as dest_airport,

-- schedule and actual times, local time as hhmm text
"CRSDepTime" as scheduled_dep_hhmm,
"DepTime" as actual_dep_hhmm,
"WheelsOff" as wheels_off_hhmm,
"CRSArrTime" as scheduled_arr_hhmm,
"ArrTime" as actual_arr_hhmm,

-- delays and durations, minutes
"DepDelay" as dep_delay_min,
"ArrDelay" as arr_delay_min,
"TaxiOut" as taxi_out_min,
"CRSElapsedTime" as scheduled_elapsed_min,
"ActualElapsedTime" as actual_elapsed_min,
"AirTime" as air_time_min,

-- delay causes, minutes (filled only for arrivals delayed 15+ minutes)
"CarrierDelay" as carrier_delay_min,
"WeatherDelay" as weather_delay_min,
"NASDelay" as nas_delay_min,
"SecurityDelay" as security_delay_min,
"LateAircraftDelay" as late_aircraft_delay_min,

-- outcome
"Cancelled" = 1 as is_cancelled,
"Diverted" = 1 as is_diverted,
"CancellationCode" as cancellation_code,
case "CancellationCode"
    when 'A' then 'carrier'
    when 'B' then 'weather'
    when 'C' then 'nas'
    when 'D' then 'security'
end as cancellation_reason,

-- distance
"Distance" as distance_miles,
round("Distance" * 1.609344, 1) as distance_km,

-- lineage
_source_file from source )

select
    -- A flight is identified by date, airline, flight number and origin.
    md5(
        concat_ws(
            '|', flight_date, airline_code, flight_number, origin_airport
        )
    ) as flight_id, *
from renamed