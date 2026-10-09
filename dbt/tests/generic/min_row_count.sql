{% test min_row_count(model, min_rows) %}
select count(*) as row_count
from {{ model }}
having count(*) < {{ min_rows }}
{% endtest %}
