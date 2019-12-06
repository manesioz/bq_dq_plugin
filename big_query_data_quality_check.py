import json
import warnings
from typing import Any, Dict, Iterable, List, Optional, SupportsAbs, Union
from collections import defaultdict

from googleapiclient.errors import HttpError
from airflow.utils.decorators import apply_defaults

from airflow.utils.decorators import apply_defaults
from airflow.contrib.operators.bigquery_operator import BigQueryValueCheckOperator

class BigQueryDataQualityCheckOperator(BigQueryValueCheckOperator):
    '''
    Automates common data quality checks.
    
    :param schema: A Schema string that takes the form 'column1:STRING,column2:INT64,column3:FLOAT64'
    :type schema": str
    
    :param metrics_thresholds: A dictionary that is filled with default data quality checks (depending on the column's data type). It takes the form of {'metric': threshold}.
    :type metrics_thresholds: Dict[str, int]
    '''
    
    @apply_defaults
    def __init__(self, schema: str = None, metrics_thresholds: dict = None, *args, **kwargs) -> None:
        super().__init__(sql=sql, *args, **kwargs)
        
        if self.schema is None:
            #auto-detect by querying and parsing as csv?
            pass
        
        else:
            self.schema = schema
            list_of_columns = []
            types_of_columns = []
            
            for char in self.schema.split(','):
                list_of_columns.append(char.split(':')[0])
                types_of_columns.append(char.split(':')[1])
                
            
        
        if self.metrics_thresholds is None:

            self.metrics_thresholds = defaultdict()
            for col, dtype in zip(list_of_columns, types_of_columns):
                if dtype == 'INT64' or dtype == 'FLOAT':
                    #process numerical dtypes
                    self.metrics_thresholds['count({})'.format(col)] = 1.9
                    self.metrics_thresholds['sum(case when {} is null then 1 else 0 end)'.format(col)] = 1.9
                    self.metrics_thresholds['avg({})'.format(col)] = 1.9
                    self.metrics_thresholds['stddev({})'.format(col)] = 1.9
                    self.metrics_thresholds['sum(case when {} = 0 then 1 else 0 end)'.format(col)] = 1.9
                    self.metrics_thresholds['min({})'.format(col)] = 1.9
                    self.metrics_thresholds['percentile_cont({}, 0.5) over()'.format(col)] = 1.9
                    self.metrics_thresholds['max({})'.format(col)] = 1.9
        
                if dtype == 'STRING':
                    #process categorical dtypes
                    self.metrics_thresholds['count({})'.format(col)] = 1.9
                    self.metrics_thresholds['sum(case when {} is null then 1 else 0 end)'.format(col)] = 1.9
                    self.metrics_thresholds['count(distinct {})'.format(col)] = 1.9
                    self.metrics_thresholds['avg(length({}))'.format(col)] = 1.9
        else:
            self.metrics_thresholds = metrics_thresholds
            
    def get_db_hook(self):
        return BigQueryHook(bigquery_conn_id=self.gcp_conn_id,
                            use_legacy_sql=self.use_legacy_sql)
