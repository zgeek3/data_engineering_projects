from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 table="",
                 redshift_conn_id="",
                 aws_credentials_id="",
                 s3_bucket="",
                 s3_key="",
                 s3_region="", # having trouble with airflow so putting it here
                 format_info="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table=table
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.s3_region=s3_region
        self.format_info=format_info

    def execute(self, context):
        # Setting up connection info
        self.log.info('Setting up redshift connection')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Setting up aws credentials')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        # Deleting previous versions of the table
        self.log.info("Deleting data from table {}".format(self.table))
        redshift.run("DELETE FROM public.{}".format(self.table))
        
        # Setting up S3 path
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        
        # Preparing copy command 
        # https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html
        # https://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-copy-from-json.html
        self.log.info('Copying S3 data to {}'.format(self.table))
        copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        region as '{}'
        {}
        ;
        """.format(self.table, s3_path, credentials.access_key, credentials.secret_key, self.s3_region, self.format_info)
        
        # Running the compy command to copy s3 info into redshift
        redshift.run(copy_sql)
        
        self.log.info('{} have been copied into redshift'.format(self.table))
        self.log.info('Everythting went well')
       
        
        


