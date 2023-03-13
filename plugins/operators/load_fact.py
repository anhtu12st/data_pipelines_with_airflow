from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    This operator loads data into a fact table in a Redshift database.

    :param redshift_conn_id: The connection ID to use for connecting to the Redshift database.
    :type redshift_conn_id: str
    :param table: The name of the fact table to load data into.
    :type table: str
    :param sql: The SQL statement used to insert data into the fact table.
    :type sql: str
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql=sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        redshift.run(f"INSERT INTO {self.table} {self.sql}")
        self.log.info(f"LoadFactOperator: Table {self.table} in Redshift")
