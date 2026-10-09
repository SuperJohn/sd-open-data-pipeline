{# Fails rows where `column_name` is earlier than `other_column` (e.g. closed before opened). #}
{% test not_before(model, column_name, other_column) %}
select *
from {{ model }}
where {{ column_name }} is not null
  and {{ other_column }} is not null
  and {{ column_name }} < {{ other_column }}
{% endtest %}
