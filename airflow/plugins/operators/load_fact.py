from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    sql = """
        INSERT INTO {table} 
        {table_input};
    """

    '''
    An operator to load data into a fact table

    Args:
        redshift_conn_id (str): The id for the Redshift cluster to connect to
        table (str): The fact table name
        table_input (str): The input data in SQL
        *args: Variable arguments
        **kwargs: Keyword arguments

    Attributes:
        redshift_conn_id (str): The id for the Redshift cluster to connect to
        table (str): The fact table name
        table_input (str): The input data in SQL
    '''
    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 table_input,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.table_input = table_input

    def execute(self, context):
        '''
        Load the input data into the fact table

        Args:
            context (dict): The Airflow context object
        '''
        self.log.info('LoadFactOperator not implemented yet')
       
        redshift = PostgresHook(self.redshift_conn_id)
        
        redshift.run(LoadDimensionOperator.sql.format(
            table=self.table,
            table_input=self.table_input
        ))
