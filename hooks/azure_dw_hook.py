from airflow.hooks import DbApiHook

import pyodbc


class AzureDWHook(DbApiHook):
    """
    """
    conn_type = 'Azure Data Warehouse'

    def __init__(self, azure_conn_id='azure_default',  *args, **kwargs):
        self.azure_conn_id = azure_conn_id

    def get_conn(self):
        """
        Fetches Azure Client
        """
        conn = self.get_connection(self.azure_conn_id)
        driver = '{ODBC Driver 13 for SQL Server}'
        config = 'DRIVER={driver};PORT=1433;SERVER={server};PORT=1443;DATABASE={database};UID={username};PWD={password}'.format(
                      driver=driver,
                      server=conn.host,
                      port='1433' if conn.port is None else conn.port,
                      database='' if conn.schema is None else conn.schema,
                      username=conn.login,
                      password=conn.password)

        cnxn = pyodbc.connect(config)
        return cnxn
