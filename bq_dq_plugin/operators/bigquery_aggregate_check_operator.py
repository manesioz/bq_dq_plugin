import json
import warnings
from typing import Any, Dict, Iterable, List, Optional, SupportsAbs, Union
from collections import defaultdict

from googleapiclient.errors import HttpError
from airflow.utils.decorators import apply_defaults

from airflow.contrib.operators.bigquery_check_operator import BigQueryIntervalCheckOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from bq_dq_plugin.operators.aggregate_check_operator import AggregateCheckOperator 

class BigQueryAggregateCheckOperator(AggregateCheckOperator):

    template_fields = ('table', 'gcp_conn_id', )
    @apply_defaults
    def __init__(self,
                 table: str,
                 metrics_thresholds: dict,
                 date_filter_column: str = 'ds',
                 agg_time_period: Optional[str] = 'Day', 
                 start_date: Optional[str], 
                 end_date: Optional[str],
                 gcp_conn_id: str = 'google_cloud_default',
                 bigquery_conn_id: Optional[str] = None,
                 use_legacy_sql: bool = True,
                 *args,
                 **kwargs) -> None:
        super().__init__(
            table=table, metrics_thresholds=metrics_thresholds,
            date_filter_column=date_filter_column, agg_time_period=agg_time_period,
            start_date=start_date, end_date=end_date, 
            *args, **kwargs)

        if bigquery_conn_id:
            warnings.warn(
                "The bigquery_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.", DeprecationWarning, stacklevel=3)
            gcp_conn_id = bigquery_conn_id

        self.gcp_conn_id = gcp_conn_id
        self.use_legacy_sql = use_legacy_sql

    def get_db_hook(self):
        return BigQueryHook(bigquery_conn_id=self.gcp_conn_id,
                            use_legacy_sql=self.use_legacy_sql)