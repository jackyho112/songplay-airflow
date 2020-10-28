from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    
    '''
    An operator to check data quality

    Args:
        redshift_conn_id (str): The id for the Redshift cluster to connect to
        tables (list): A list of table names to check for
        checks (list): A list of check functions for the data
        *args: Variable arguments
        **kwargs: Keyword arguments

    Attributes:
        redshift_conn_id (str): The id for the Redshift cluster to connect to
        tables (list): A list of table names to check for
        checks (list): A list of check functions for the data
    '''
    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 tables=[],
                 checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.checks = checks

    def execute(self, context):
        '''
        Check if the table is empty

        Args:
            context (dict): The Airflow context object
        '''
        redshift_hook = PostgresHook(self.redshift_conn_id)

        message = ''
        
        for table in self.tables:
            for check in self.checks:
                message += check(redshift_hook, table)

        if len(message) > 0:
            raise ValueError(message)
            