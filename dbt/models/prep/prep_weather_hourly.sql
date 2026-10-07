-- One row per airport and local hour, with the weather observed during that hour.
--
-- Hour assignment: a routine METAR is issued around :53 and describes the hour that follows,
-- so reports are shifted by 7 minutes before truncating. The hour 11:00 therefore collects
-- the routine report from 10:53 and all special reports up to 11:52.
--
-- Every hour of every month that has data gets a row, even without reports,
-- so gaps stay visible (has_report = false, weather_group = NULL).


with reports as (

    select
        *,
        date_trunc('hour', valid_local + interval 7 minute) as weather_hour,

-- Drop codes that do not describe weather at the airport itself:
-- VC = in the vicinity, BL = blowing, DR = low drifting (snow lifted by wind).


list_filter(
            string_split(coalesce(weather_codes, ''), ' '),
            code -> code <> '' and not regexp_matches(code, '^[-+]?(VC|BL|DR)')
        ) as codes_at_airport

    from {{ ref('stg_metar') }}

),

flagged as (

    select
        *,
        array_to_string(codes_at_airport, ' ') as codes_text
    from reports

),

phenomena as (

    select
        *,
        regexp_matches(codes_text, 'FZRA|FZDZ')      as has_freezing_rain,
        regexp_matches(codes_text, 'PL')             as has_ice_pellets,
        regexp_matches(codes_text, 'SN|SG')          as has_snow,
        -- rain or drizzle that is not freezing: remove FZRA/FZDZ first (RE2 has no lookbehind)
        regexp_matches(replace(replace(codes_text, 'FZRA', ''), 'FZDZ', ''), 'RA|DZ') as has_rain,
        regexp_matches(codes_text, 'FG')             as has_fog,
        regexp_matches(codes_text, 'TS')             as has_thunder
    from flagged

),

hourly as (

    select
        airport_code,
        weather_hour,

        count(*)                                          as n_reports,
        count(*) filter (where report_type = 'special')   as n_special_reports,

-- values from the routine report of the hour
arg_max (temp_c, valid_utc) filter (
    where
        report_type = 'routine'
) as temp_c,
arg_max (dewpoint_c, valid_utc) filter (
    where
        report_type = 'routine'
) as dewpoint_c,
-- precipitation in a routine report covers the whole previous hour;
-- special reports carry running totals and must not be summed
arg_max (precip_mm, valid_utc) filter (
    where
        report_type = 'routine'
) as precip_mm,

-- extremes across all reports of the hour
min(temp_c) as temp_min_c,
max(wind_speed_kmh) as wind_speed_max_kmh,
max(wind_gust_kmh) as wind_gust_max_kmh,
min(visibility_km) as visibility_min_km,
max(ice_accretion_1h_mm) as ice_accretion_max_mm,

-- a phenomenon counts for the hour if any report of the hour mentions it


bool_or(has_freezing_rain)  as has_freezing_rain,
        bool_or(has_ice_pellets)    as has_ice_pellets,
        bool_or(has_snow)           as has_snow,
        bool_or(has_rain)           as has_rain,
        bool_or(has_fog)            as has_fog,
        bool_or(has_thunder)        as has_thunder,

        string_agg(distinct nullif(codes_text, ''), ' | ') as weather_codes_hour

    from phenomena
    group by airport_code, weather_hour

),

-- every local hour of every month that has data, per airport
hour_grid as (
    select months.airport_code, unnest (
            generate_series (
                months.month_start, months.month_start + interval 1 month - interval 1 hour, interval 1 hour
            )
        ) as weather_hour
    from (
            -- months come from the report time itself, not from the shifted weather_hour:
            -- a report at 23:53 on the last day of a month would otherwise add the next month
            select distinct
                airport_code, date_trunc ('month', valid_local) as month_start
            from reports
        ) as months
)

select
    md5(
        concat_ws(
            '|',
            grid.airport_code,
            grid.weather_hour
        )
    ) as weather_hour_id,
    grid.airport_code,
    grid.weather_hour,
    cast(grid.weather_hour as date) as weather_date,
    hourly.n_reports is not null as has_report,
    coalesce(hourly.n_reports, 0) as n_reports,
    coalesce(hourly.n_special_reports, 0) as n_special_reports,
    hourly.temp_c,
    hourly.dewpoint_c,
    hourly.temp_min_c,
    hourly.precip_mm,
    hourly.wind_speed_max_kmh,
    hourly.wind_gust_max_kmh,
    hourly.visibility_min_km,
    hourly.ice_accretion_max_mm,
    hourly.has_freezing_rain,
    hourly.has_ice_pellets,
    hourly.has_snow,
    hourly.has_rain,
    hourly.has_fog,
    hourly.has_thunder,
    hourly.has_freezing_rain
    or hourly.has_ice_pellets
    or hourly.has_snow as has_winter_precip,

-- one label per hour, the most disruptive phenomenon first
case
    when hourly.n_reports is null then null
    when hourly.has_freezing_rain then 'freezing_rain'
    when hourly.has_ice_pellets then 'ice_pellets'
    when hourly.has_snow then 'snow'
    when hourly.has_rain then 'rain'
    when hourly.has_fog then 'fog'
    else 'none'
end as weather_group,
hourly.weather_codes_hour
from hour_grid as grid
    left join hourly on hourly.airport_code = grid.airport_code
    and hourly.weather_hour = grid.weather_hour