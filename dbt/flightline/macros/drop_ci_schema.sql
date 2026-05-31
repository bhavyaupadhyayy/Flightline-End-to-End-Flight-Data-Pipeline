{% macro drop_ci_schema() %}
    {% set schema_name = target.schema %}
    {% if 'CI_' in schema_name %}
        {% do run_query('drop schema if exists ' ~ target.database ~ '.' ~ schema_name ~ ' cascade') %}
        {{ log('dropped ' ~ schema_name, info=True) }}
    {% endif %}
{% endmacro %}
