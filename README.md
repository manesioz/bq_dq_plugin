# bq_dq_plugin
[WIP] Airflow plug-in that allows you to compare various metrics based on aggregated historical metrics and arbitrary thresholds. 

Previously, Airflow's `BigQueryIntervalCheckOperator` allows you to compare the current day's metrics with the metrics 
of another day. This extends that functionality and allows you to compare it to aggregated metrics over an arbitrary window. 

### Example 
Previously, you could run the following query: 

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
