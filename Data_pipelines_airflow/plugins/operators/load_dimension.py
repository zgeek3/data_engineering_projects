from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 table="",
                 sql_to_run="",
                 redshift_conn_id="",
                 append_status="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table=table
        self.redshift_conn_id=redshift_conn_id
        self.sql_to_run=sql_to_run
        self.append_status=append_status
        

    def execute(self, context):
        self.log.info('Setting up redshift connection')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        
        # Deleting previous versions of the table if append_status = False
        if self.append_status==False:
            self.log.info("Deleting data from table {}".format(self.table))
            redshift.run("DELETE FROM public.{}".format(self.table))
        
        insert_sql = """
        INSERT INTO {}
        {}
        ;
        """.format(self.table, self.sql_to_run)
        
        
        # Running the desired sql command
        self.log.info("This is the query {}".format(insert_sql))
        redshift.run(insert_sql)
        
        
        self.log.info('{} has been updated'.format(self.table))
        self.log.info('Everything went well')
        
