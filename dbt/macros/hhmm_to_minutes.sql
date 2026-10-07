#
{#
    Convert a BTS local time in hhmm text format ('0605', '2400') to minutes after midnight.
    '2400' becomes 1440, i.e. midnight at the end of the day. NULL stays NULL.
#}
{% macro hhmm_to_minutes(column) %}
    (
        cast(substr(lpad({{ column }}, 4, '0'), 1, 2) as integer) * 60
        + cast(substr(lpad({{ column }}, 4, '0'), 3, 2) as integer)
    )
{% endmacro %}