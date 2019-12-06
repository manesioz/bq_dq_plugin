# bq_dq_plugin
Airflow plug-in that extends the `BigQueryIntervalCheckOperator` to automate Data Quality checks. 

Out-of-the-box checks: 

### Numerical ###
- num_records 
- percent_null
- mean
- std_dev
- num_zero
- min
- median
- max 

### Categorical ### 
- num_records
- percent_null
- num_unique 
- top
- top_freq
- avg_str_len

This plugin will allow you to create more complex custom checks as well. 
