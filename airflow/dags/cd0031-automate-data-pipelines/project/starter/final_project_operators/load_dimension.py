from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """Loads data into a dimension table in Redshift.
    This operator executes a SQL statement to load data into a Redshift
    dimension table. The SQL statement can be provided as a parameter or as
    a template file (using the `templates_dict` argument). By default, the
    operator appends new data to the existing table, but it can also truncate
    the table before loading the data (using the `truncate` argument).
    :param redshift_conn_id: The Redshift connection ID configured in Airflow.
    :type redshift_conn_id: str
    :param table: The name of the dimension table.
    :type table: str
    :param sql: The SQL statement to execute to load the data.
    :type sql: str
    :param truncate: If `True`, truncate the table before loading the data.
        If `False`, append new data to the table (default).
    :type truncate: bool
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 truncate=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.truncate = truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate:
            formatted_sql = f"TRUNCATE {self.table}; INSERT INTO {self.table} ({self.sql})"
        else:
            formatted_sql = f"INSERT INTO {self.table} ({self.sql})"
            
        redshift.run(formatted_sql)
        
        self.log.info(f'LoadDimensionOperator: Table {self.table}')
