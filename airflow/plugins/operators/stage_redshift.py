from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}'
        COMPUPDATE OFF;
    """

    '''
    Load JSON data into a staging table

    Args:
        aws_credentials_id (str): The id for the aws credentials
        table (str): The staging table name
        redshift_conn_id (str): The id for the Redshift cluster to connect to
        s3_bucket (str): The bucket for the JSON files
        s3_key (str): The key for the JSON files
        json_path (str): The path to the JSON format file or 'auto' for auto-formatting
        *args: Variable arguments
        **kwargs: Keyword arguments

    Attributes:
        aws_credentials_id (str): The id for the aws credentials
        table (str): The staging table name
        redshift_conn_id (str): The id for the Redshift cluster to connect to
        s3_bucket (str): The bucket for the JSON files
        s3_key (str): The key for the JSON files
        json_path (str): The path to the JSON format file or 'auto' for auto-formatting
    '''
    @apply_defaults
    def __init__(self,
                 aws_credentials_id,
                 table,
                 redshift_conn_id,
                 s3_bucket,
                 s3_key,
                 json_path,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path

    def execute(self, context):
        '''
        Load the JSON data into the staging table

        Args:
            context (dict): The Airflow context object
        '''
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        
        redshift.run(StageToRedshiftOperator.sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_path
        ))
