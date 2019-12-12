# bq_dq_plugin
Airflow plug-in that allows you to compare various metrics based on aggregated historical metrics and arbitrary thresholds.  

### Why? 

Previously, Airflow's `BigQueryIntervalCheckOperator` allowed you to compare the current day's metrics with the metrics 
of another day. This extends that functionality and allows you to compare it to aggregated metrics over an arbitrary window.
This is import for more robust data quality checks that are more resilient to noise and daily flucuation. 

You could run the following query: 

```sql
select count(*) as NumRecords, max(column1) as MaxColumn_1 
  from table
 where date_column = '{{ds}}'
```

which computes the number of rows and the max of column1 for today. 
Now, compare it with the same metrics computed 7 days earlier:

```sql
select count(*), max(column1) 
  from {{table}}
 where date_column = timestamp_sub('{{ds}}', interval 7 day) 
```

However, if you want more robust data quality checks you likely want to compare today's metric
with an aggregated value (since that is less susceptible to daily flucutations/noise). 

With this library, you can compare: 

```sql
select count(*) as NumRecords, max(column1) as MaxColumn_1 
  from table
 where timestamp_trunc(date_column, Day) = timestamp_trunc('2019-01-01', Day) 
```

which computes the number of records and the max of column1 for records that have `date_column = '2019-01-01'`. 
Now, let's compare the aggregated values: 

```sql
with data as (
    select count(*) as NumRecords, max(column1) as MaxColumn_1, timestamp_trunc(date_column, Day) as Time
      from table 
     where date_column between '2018-01-01' and '2018-12-31' 
     group by Time
)

select avg(NumRecords), avg(MaxColumn_1)
  from data 
```

Like the `BigQueryIntervalCheckOperator`, you can pass a `dict` which contains all metrics and their associated thresholds as key/value pairs. 

In addition to this functionality, there will be some out-of-the-box checks including: 

### Numerical ###
- `num_records` 
- `percent_null`
- `mean`
- `std_dev`
- `num_zero`
- `min`
- `median`
- `max`

### Categorical ### 
- `num_records`
- `percent_null`
- `num_unique` 
- `top`
- `top_freq`
- `avg_str_len`

This plugin will allow you to create more complex custom checks as well. 

### Example usage (in a DAG) 

```python
from airflow import DAG
from datetime import datetime, timedelta
from bq_dq_plugin.operators.big_query_aggregate_check_operator import BigQueryAggregateCheckOperator

default_args = {
    'owner': 'Zachary Manesiotis', 
    'depends_on_past': False, 
    'start_date': datetime(2019, 12, 10), 
    'email': ['zacl.manesiotis@gmail.com'], 
    'email_on_failure': True, 
    'email_on_retry': False, 
    'retries': 3, 
    'retry_delay': timedelta(minutes=1)
} 

with DAG('Dag_ID', schedule_interval='@weekly', max_active_runs=15, catchup=False, default_args=default_args) as dag: 
    data_quality_check = BigQueryAggregateCheckOperator(
        task_id='data_quality_check', 
        table='`project.dataset.table`',
        metrics_thresholds={'count(*)': 1.5, 'max(Column1)': 1.6}, 
        date_filter_column='DateTime', 
        agg_time_period='Day', 
        start_date='2019-01-01', 
        end_date='2019-12-01', 
        gcp_conn_id=CONNECTION_ID, 
        use_legacy_sql=False, 
        dag=dag
    )
data_quality_check
```
