#
{#
    Convert a METAR text value to a number.
    IEM marks missing values with 'M' and trace amounts (precipitation, ice) with 'T'.
    - 'M' becomes NULL.
    - 'T' becomes trace_value (default NULL;

use 0 for precipitation and ice accretion).
    Any other non-numeric text makes the cast fail, so unexpected values are not hidden.
#}
{% macro metar_number(column, trace_value='null') %}
    case
        when {{ column }} = 'M' then null
        when {{ column }} = 'T' then {{ trace_value }}
        else cast({{ column }} as double)
    end
{% endmacro %}