from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    truncate_sql = """
        TRUNCATE TABLE {table};
    """
    
    append_sql = """
        INSERT INTO {table} 
        {table_input};
    """

    '''
    An operator to load data into a dimension table

    Args:
        redshift_conn_id (str): The id for the Redshift cluster to connect to
        table (str): The dimension table name
        table_input (str): The input data in SQL
        append_only (bool): Whether to just append data
        *args: Variable arguments
        **kwargs: Keyword arguments

    Attributes:
        redshift_conn_id (str): The id for the Redshift cluster to connect to
        table (str): The dimension table name
        table_input (str): The input data in SQL
        append_only (bool): Whether to just append data
    '''
    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 table_input,
                 append_only=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.table_input = table_input
        self.append_only = append_only

    def execute(self, context):
        '''
        Load the input data into the dimension table

        Args:
            context (dict): The Airflow context object
        '''
        redshift = PostgresHook(self.redshift_conn_id)
        
        if not self.append_only:
            redshift.run(LoadDimensionOperator.truncate_sql.format(
                table=self.table
            ))
        
        redshift.run(LoadDimensionOperator.append_sql.format(
            table=self.table,
            table_input=self.table_input
        ))
