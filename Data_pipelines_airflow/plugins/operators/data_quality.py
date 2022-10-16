from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 tables=[],
                 redshift_conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.tables=tables
        self.redshift_conn_id=redshift_conn_id

    def execute(self, context):
        
        self.log.info('Setting up redshift connection')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        minimum_count=0
        
        for table in self.tables:
            table_length_query = """
            SELECT count(*) from {}
            ;
            """.format(table)
            self.log.info("Getting the number of rows for table:   {}".format(table))
            records = redshift.get_records(table_length_query)[0]
            self.log.info("The number of records for table:   {} is {}".format(table,records[0]))
            if records[0] < minimum_count:
                self.log.info("The number of records for table:  {} is {} which is less than the minimum count: {}".format(table,records[0],minimum_count))
                raise ValueError("Data quality check failed.")
        self.log.info("The data quality checks have passed.")
            