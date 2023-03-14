from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend

class StageToRedshiftOperator(BaseOperator):
    """
    This operator copies data from S3 to a staging table in a Redshift database.
    :param redshift_conn_id: The connection ID to use for connecting to the Redshift database.
    :type redshift_conn_id: str
    :param aws_credentials_id: The connection ID to use for accessing AWS credentials.
    :type aws_credentials_id: str
    :param table: The name of the staging table in the Redshift database.
    :type table: str
    :param s3_bucket: The name of the S3 bucket where the data is stored.
    :type s3_bucket: str
    :param s3_key: The key of the file in S3 containing the data to be copied.
    :type s3_key: str
    :param ignore_headers: The number of header rows to ignore when copying data from S3. Default value is 1.
    :type ignore_headers: int
    :param json_path_option: The JSON path option used when copying JSON data from S3. Default value is 'auto'.
    :type json_path_option: str
    """
    
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        FORMAT as json '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 ignore_headers=1,
                 json_path_option="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.ignore_headers = ignore_headers
        self.aws_credentials_id = aws_credentials_id
        self.json_path_option = json_path_option

    def execute(self, context):
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection(self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            aws_connection.login,
            aws_connection.password,
            self.ignore_headers,
            self.json_path_option
        )
        redshift.run(formatted_sql)

        self.log.info(f'StageToRedshiftOperator: Table {self.table} in Amazon Redshift')
