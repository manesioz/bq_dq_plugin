import json
import warnings
from collections import defaultdict
from typing import Any, Dict, Iterable, Optional, SupportsAbs

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class AggregateCheckOperator(BaseOperator):

    __mapper_args__ = {
        'polymorphic_identity': 'AggregateCheckOperator'
    }
    template_fields = ('sql1', 'sql2')  # type: Iterable[str]
    template_ext = ('.hql', '.sql',)  # type: Iterable[str]
    ui_color = '#fff7e6'

    ratio_formulas = {
        'max_over_min': lambda cur, ref: float(max(cur, ref)) / min(cur, ref),
        'relative_diff': lambda cur, ref: float(abs(cur - ref)) / ref,
    }

    @apply_defaults
    def __init__(
        self,
        table: str,
        metrics_thresholds: Dict[str, int],
        date_filter_column: Optional[str] = 'DateTime',
        agg_time_period: Optional[str] = 'Day',
        start_date: Optional[str], 
        end_date: Optional[str],
        ratio_formula: Optional[str] = 'max_over_min',
        ignore_zero: Optional[bool] = True,
        conn_id: Optional[str] = None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        if ratio_formula not in self.ratio_formulas:
            msg_template = "Invalid diff_method: {diff_method}. " \
                           "Supported diff methods are: {diff_methods}"

            raise AirflowException(
                msg_template.format(diff_method=ratio_formula,
                                    diff_methods=self.ratio_formulas)
            )
        self.ratio_formula = ratio_formula
        self.ignore_zero = ignore_zero
        self.table = table
        self.metrics_thresholds = metrics_thresholds
        self.metrics_sorted = sorted(metrics_thresholds.keys())
        self.date_filter_column = date_filter_column
        self.agg_time_period = agg_time_period
        self.start_date = start_date
        self.end_date = end_date
        self.conn_id = conn_id
        sqlexp = ', '.join(self.metrics_sorted)
        sql_test = 'SELECT {sqlexp} FROM {table} WHERE TIMESTAMP_TRUNC({date_filter_column}, {agg_time_period})='.format(
            sqlexp=sqlexp, table=table, date_filter_column=date_filter_column, agg_time_period=agg_time_period
        )

        result = ''
        alias = [] 
        for i, metric in enumerate(self.metrics_sorted): 
            result += '{0} as {1}, '.format(metric, ''.join(ch for ch in metric if ch.isalnum()))
            alias.append(''.join(ch for ch in metric if ch.isalnum()))
        sqlexp = result[:-2]
        final = ''
        for name in alias: 
            final += 'avg({}), '.format(name)
        agg_metrics = final[:-2]
        sql_agg = '''with init_data as (
        select {sqlexp}, TIMESTAMP_TRUNC({date_filter_column}, {agg_time_period}) as AGG_TIME 
          from {table}
          where  {date_filter_column} between {start_time} and {end_time}
        )
        select {agg_metrics} from init_data'''.format(sqlexp=sqlexp, date_filter_column=date_filter_column, table=table,
            agg_time_period=agg_time_period, start_time=start_time, end_time=end_time, agg_metrics=agg_metrics)

        self.sql1 = sql_test + "TIMESTAMP_TRUNC('{{ ds }}', {{agg_time_period}})"  
        self.sql2 = sql_agg

    def execute(self, context=None):
        hook = self.get_db_hook()
        self.log.info('Using ratio formula: %s', self.ratio_formula)
        self.log.info('Executing SQL check: %s', self.sql2)
        row2 = hook.get_first(self.sql2)
        self.log.info('Executing SQL check: %s', self.sql1)
        row1 = hook.get_first(self.sql1)

        if not row2:
            raise AirflowException("The query {} returned None".format(self.sql2))
        if not row1:
            raise AirflowException("The query {} returned None".format(self.sql1))

        current = dict(zip(self.metrics_sorted, row1))
        reference = dict(zip(self.metrics_sorted, row2))

        ratios = {}
        test_results = {}

        for m in self.metrics_sorted:
            cur = current[m]
            ref = reference[m]
            threshold = self.metrics_thresholds[m]
            if cur == 0 or ref == 0:
                ratios[m] = None
                test_results[m] = self.ignore_zero
            else:
                ratios[m] = self.ratio_formulas[self.ratio_formula](current[m], reference[m])
                test_results[m] = ratios[m] < threshold

            self.log.info(
                (
                    "Current metric for %s: %s\n"
                    "Past metric for %s: %s\n"
                    "Ratio for %s: %s\n"
                    "Threshold: %s\n"
                ), m, cur, m, ref, m, ratios[m], threshold)

        if not all(test_results.values()):
            failed_tests = [it[0] for it in test_results.items() if not it[1]]
            j = len(failed_tests)
            n = len(self.metrics_sorted)
            self.log.warning("The following %s tests out of %s failed:", j, n)
            for k in failed_tests:
                self.log.warning(
                    "'%s' check failed. %s is above %s", k, ratios[k], self.metrics_thresholds[k]
                )
            raise AirflowException("The following tests have failed:\n {0}".format(", ".join(
                sorted(failed_tests))))

        self.log.info("All tests have passed")

    def get_db_hook(self):
        return BaseHook.get_hook(conn_id=self.conn_id)