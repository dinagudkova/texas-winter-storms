-- One row per METAR report (routine or special) from the IEM archive.
-- Staging converts text to numbers, US units to metric and UTC to local time.
-- Parsing weather codes and hourly aggregation happen in prep.


with source as (

    select * from {{ source('raw', 'metar') }}

),

converted as (

    select
        -- identifiers and time
        airport_code,
        report_type,
        valid_utc,
        timezone('{{ var("local_timezone") }}', valid_utc at time zone 'UTC') as valid_local,

-- temperature and humidity
round(({{ metar_number('tmpf') }} - 32) * 5 / 9, 1)      as temp_c,
        round(({{ metar_number('dwpf') }} - 32) * 5 / 9, 1)      as dewpoint_c,
        {{ metar_number('relh') }}                               as rel_humidity_pct,

-- wind
{{ metar_number('drct') }}                               as wind_dir_deg,
        round({{ metar_number('sknt') }} * 1.852, 1)             as wind_speed_kmh,
        round({{ metar_number('gust') }} * 1.852, 1)             as wind_gust_kmh,

-- visibility and clouds
round({{ metar_number('vsby') }} * 1.609344, 2)          as visibility_km,
        nullif(skyc1, 'M')                                       as sky_cover_lowest,
        round({{ metar_number('skyl1') }} * 0.3048)              as cloud_base_lowest_m,

-- precipitation since the last routine report and ice accretion over the last hour;
-- a trace counts as 0 mm and is flagged separately
round({{ metar_number('p01i', trace_value='0') }} * 25.4, 2)              as precip_mm,
        p01i = 'T'                                                                  as is_precip_trace,
        round({{ metar_number('ice_accretion_1hr', trace_value='0') }} * 25.4, 2) as ice_accretion_1h_mm,
        ice_accretion_1hr = 'T'                                                     as is_ice_accretion_trace,

-- present weather codes, e.g. '-FZRA BR'; NULL when no phenomenon was reported
nullif(wxcodes, 'M') as weather_codes,

-- original report, for checking the parsed values
metar                                                    as raw_metar

    from source

)

select md5(
        concat_ws(
            '|', airport_code, valid_utc, report_type
        )
    ) as metar_id, *
from converted