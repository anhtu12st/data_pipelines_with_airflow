from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    An operator that performs data quality checks on Redshift tables.
    :param redshift_conn_id: The connection ID to the Redshift database.
    :type redshift_conn_id: str
    :param quality_checks: A list of dictionaries specifying the quality checks to perform. 
    """
    
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 quality_checks=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.quality_checks = quality_checks

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)

        records = redshift.get_records("SELECT COUNT(*) FROM public.songplays ")

        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError("Data quality check failed. songplays table returned no results")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError("Data quality check failed. songplays contained 0 rows")
        self.log.info(f"DataQualityOperator implemented: Data quality on table songplays check passed with {num_records} records")

        if self.quality_checks is not None:
            check_index = 0
            try:
                for check in self.quality_checks:
                    check_query = check['query']
                    check_result = redshift.get_records(check_query)
                    expected_result = check.get('expected_result', 0)  # get check['expected_result'] if exists, else assume 0
                    
                    if check_result == expected_result:
                        self.log.info(f"additional data quality ckeck {check_index} passed")
                    else:
                        self.log.info(f"additional data quality ckeck {check_index} failed, returned: {check_result}, expected: {expected_result}")
                    check_index += 1
                    
            except NameError:
                raise NameError("wrong data type for data qualtiy checks")
