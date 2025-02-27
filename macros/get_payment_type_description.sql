 {#
    This macro returns the description of the payment type based on cleaned_payment_type.
#}
{% macro get_payment_type_description(column_name) -%}
  CASE
    WHEN {{ column_name }} = 1 THEN 'Credit card'
    WHEN {{ column_name }} = 2 THEN 'Cash'
    WHEN {{ column_name }} = 3 THEN 'No charge'
    WHEN {{ column_name }} = 4 THEN 'Dispute'
    WHEN {{ column_name }} = 5 THEN 'Unknown'
    WHEN {{ column_name }} = 0 THEN 'Voided trip'
    ELSE 'EMPTY'  
  END
{%- endmacro %}
