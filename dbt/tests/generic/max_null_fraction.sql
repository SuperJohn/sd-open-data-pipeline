{# Completeness check: fail when more than `max_fraction` of values are null. #}
{% test max_null_fraction(model, column_name, max_fraction) %}
select
    count(*) as total_rows,
    count(*) filter (where {{ column_name }} is null) as null_rows
from {{ model }}
having count(*) > 0
   and count(*) filter (where {{ column_name }} is null) * 1.0 / count(*) > {{ max_fraction }}
{% endtest %}
