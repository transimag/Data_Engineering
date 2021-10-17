from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
import os


class PostgresToDiskOperator(PostgresOperator):
    def __init__(self, file_path, table_name, sql, postgres_conn_id='pg_conn', autocommit=False,
            parameters=None,
            database=None, *args, **kwargs):
        super(PostgresOperator, self).__init__(*args, **kwargs)
        self.file_path = file_path
        self.table_name = table_name
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id
        self.autocommit = autocommit
        self.parameters = parameters
        self.database = database

    def execute(self, context):
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id, schema=self.database)

        self.log.info("Calling postgres table")

        cursor = pg_hook.get_conn().cursor()
        with open(file=os.path.join(self.file_path, self.table_name +'.csv'), mode='w') as csv_file:
            cursor.copy_expert(self.sql, csv_file)




