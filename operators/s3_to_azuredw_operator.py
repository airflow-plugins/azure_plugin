from airflow.models import BaseOperator
from airflow.hooks.S3_hook import S3Hook
from AzureDWPlugin.hooks.azure_dw_hook import AzureDWHook

from airflow.utils.decorators import apply_defaults

import json
import logging
import collections
import re
import string
import random


class S3ToAzureDWOperator(BaseOperator):
    """
    S3 To Azure DW Operator
    :param s3_conn_id:              The source s3 connection id.
    :type s3_conn_id:               string
    :param s3_bucket:               The source s3 bucket.
    :type s3_bucket:                string
    :param s3_key:                  The source s3 key.
    :type s3_key:                   string
    :param azure_conn_id:           The destination azure connection id.
    :type azure_conn_id:            string
    :param azure_schema:            The destination azure schema.
    :type azure_schema:             string
    :param table:                   The destination azure table.
    :type table:                    string
    :param origin_schema:           The incoming data schema.
                                    Expects a JSON file with a single dict
                                    specifying column and datatype as a
                                    key-value pair. (e.g. "column1":"int(11)")
    :type origin_schema:            string
    :param load_type:               The method of loading into Azure that
                                    should occur. Options are "append",
                                    "rebuild", and "upsert". Defaults to
                                    "append."
    :type load_type:                string
    :param primary_key:             *(optional)* The primary key for the
                                    destination table. Only required if using
                                    a load_type of "upsert".
    :type primary_key:              string
    :param incremental_key:         *(optional)* The incremental key to compare
                                    new data against the destination table
                                    with. Only required if using a load_type of
                                    "upsert".
    :type incremental_key:          string
    """

    template_fields = ('s3_key',
                       'origin_schema')

    @apply_defaults
    def __init__(self,
                 s3_conn_id,
                 s3_bucket,
                 s3_key,
                 azure_conn_id,
                 azure_schema,
                 azure_table,
                 origin_schema=None,
                 schema_location='s3',
                 origin_datatype=None,
                 load_type='append',
                 primary_key=None,
                 incremental_key=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.azure_conn_id = azure_conn_id
        self.azure_schema = azure_schema
        self.azure_table = re.sub(r'-', r'_', azure_table)
        self.origin_schema = origin_schema
        self.origin_datatype = origin_datatype
        self.load_type = load_type
        self.primary_key = [key.lower() for key in primary_key] if primary_key is not None else None
        self.incremental_key = incremental_key.lower() if incremental_key is not None else None

        if self.load_type.lower() not in ["append", "rebuild", "upsert"]:
            raise Exception('Please choose "append", "rebuild", or "upsert".')

        if self.incremental_key is not None and self.primary_key is None:
            raise Exception('Cannot set incremental key without a primary key.')

    def execute(self, context):
        m_hook = AzureDWHook(self.azure_conn_id)
        data = (S3Hook(self.s3_conn_id)
                .get_key(self.s3_key, bucket_name=self.s3_bucket)
                .get_contents_as_string(encoding='utf-8'))

        self.copy_data(m_hook, data)

    def copy_data(self, m_hook, data):
        if self.load_type == 'rebuild':
            drop_query = \
                """
                DROP TABLE IF EXISTS {schema}.{table}
                """.format(schema=self.azure_schema, table=self.azure_table)
            m_hook.run(drop_query)

        table_exists_query = \
            """
            SELECT *
            FROM information_schema.tables
            WHERE table_schema = '{schema}' AND table_name = '{table}'
            """.format(schema=self.azure_schema, table=self.azure_table)

        schema_exists_query = \
            """
            SELECT *
            FROM information_schema.schemata
            WHERE schema_name = '{schema}'
            """.format(schema=self.azure_schema)

        # Check to see if the table already exists. If not, the following
        # command will return an empty array, triggering a build process.
        if not m_hook.get_records(table_exists_query):
            if not m_hook.get_records(schema_exists_query):
                m_hook.run('CREATE SCHEMA {schema}'.format(schema=self.azure_schema))
            self.create_table(m_hook, self.azure_table)
            self.write_data(m_hook, data, self.azure_table)
        else:
            # If the table does exist, trigger a reconciliation process
            # to ensure the columns are the same.
            self.reconcile_schemas(m_hook)
            if self.load_type == 'upsert':
                letters = string.ascii_lowercase
                random_string = ''.join(random.choice(letters) for _ in range(7))
                temp_suffix = '_tmp_{0}'.format(random_string)
                temp_table = ''.join([self.azure_table, temp_suffix])
                self.create_table(m_hook, temp_table)
                self.write_data(m_hook, data, temp_table, upsert=True)
            else:
                self.write_data(m_hook, data, self.azure_table)

    def create_table(self, m_hook, table):
        fields = ['{name} {type} {nullable}'.format(name=field['name'].lower(),
                                                    type=field['type'].lower(),
                                                    nullable='NOT NULL'
                                                    if field['name'].lower()
                                                    in self.primary_key
                                                    else 'NULL')
                  for field in self.origin_schema]

        keys = ', '.join(self.primary_key)
        create_query = \
            """
            IF NOT EXISTS
                (SELECT *
                 FROM information_schema.tables
                 WHERE table_schema = '{schema}'
                 AND table_name = '{table}')
            BEGIN
            CREATE TABLE {schema}.{table} ({fields}
            """.format(schema=self.azure_schema,
                       table=table,
                       fields=', '.join(fields))
        if keys:
            create_query += ", PRIMARY KEY ({keys})".format(keys=keys)

        create_query += ') END;'

        m_hook.run(create_query)

    def reconcile_schemas(self, m_hook):
            column_query = \
                """
                SELECT COLUMN_NAME
                FROM information_schema.columns
                WHERE table_schema = '{schema}' AND table_name = '{table}'
                """.format(schema=self.azure_schema, table=self.azure_table)

            records = m_hook.get_records(column_query)
            existing_columns_names = [x[0].lower() for x in records]
            incoming_column_names = [field['name'].lower() for field in self.origin_schema]

            missing_columns = list(set(incoming_column_names) -
                                   set(existing_columns_names))
            if len(missing_columns):
                logging.info('Missing Columns: ' + str(missing_columns))
                columns = ['{name} {type} NULL'.format(name=field['name'].lower(),
                                                       type=field['type'])
                           for field in self.origin_schema
                           if field['name'].lower() in missing_columns]

                alter_query = \
                    """
                    ALTER TABLE {schema}.{table} ADD {columns}
                    """.format(schema=self.azure_schema,
                               table=self.azure_table,
                               columns=', '.join(columns))

                m_hook.run(alter_query)
                logging.info('The new columns were:' + str(missing_columns))
            else:
                logging.info('There were no new columns.')

    def write_data(self, m_hook, data, loading_table, upsert=False):
        position_query = \
          """
          SELECT COLUMN_NAME
          FROM information_schema.columns
          WHERE table_schema = '{schema}' AND table_name = '{table}'
          ORDER BY ORDINAL_POSITION ASC
          """.format(schema=self.azure_schema,
                     table=loading_table)

        positions = m_hook.get_records(position_query)
        existing_columns = [e[0] for e in positions]
        target_fields = ', '.join([field.lower() for field in existing_columns])
        placeholders = ','.join(['?' for i in range(len(existing_columns))])

        insert_query = \
          """
          INSERT INTO {schema}.{table} ({columns})
          VALUES ({placeholders})
          """.format(schema=self.azure_schema,
                     table=loading_table,
                     columns=target_fields,
                     placeholders=placeholders)
        # Split the incoming JSON newlines string along new lines.
        # Remove cases where two or more '\n' results in empty entries.
        records = [json.loads(record) for record in data.split('\n') if record]
        # Create a default "record" object with all available fields
        # intialized to None. These will be overwritten with the proper
        # field values as available.

        # Create a default "record" object with all available fields
        # intialized to None. These will be overwritten with the proper
        # field values as available.

        default_object = collections.OrderedDict()

        for field in existing_columns:
            default_object[field.lower()] = None

        print(default_object)

        # Initialize null to Nonetype for incoming null values in records dict
        null = None
        output = []
        keys = []
        for record in records:
            for key in list(record.keys()):
                if key not in keys:
                    keys.append(key)
                if key == 'table':
                    record['_table'] = record.pop('table')
            line_object = default_object.copy()
            line_object.update(record)
            output.append([v for k, v in line_object.items()])

        logging.info('Key Count: ' + str(len(keys)))
        logging.info('Field Count: ' + str(len(existing_columns)))
        logging.info('Missing Keys: ' + str(list(set(keys) - set(existing_columns))))

        conn = m_hook.get_conn()
        cur = conn.cursor()
        cur.executemany(insert_query, output)
        cur.close()
        conn.commit()
        conn.close()

        if upsert is True:
            # Add IF check to ensure that the records being inserted have an
            # incremental_key with a value greater than the existing records.
            source_fields = ', '.join(['S.' + field for field in existing_columns])
            primary_keys = ' AND '.join("T.{0} = S.{0}".format(pk) for pk in self.primary_key)
            update_fields = ','.join("T.{0} = S.{0}".format(field) for field in existing_columns)

            update_query = \
                """
                MERGE {schema}.{dest_table} AS T
                USING {schema}.{temp_table} AS S
                ON ({pk})
                WHEN NOT MATCHED BY TARGET
                    THEN INSERT({target_fields}) VALUES({source_fields})
                WHEN MATCHED AND (T.{ik} <= S.{ik})
                    THEN UPDATE SET {update_fields};
                """.format(schema=self.azure_schema,
                           temp_table=loading_table,
                           dest_table=self.azure_table,
                           pk=primary_keys,
                           ik=self.incremental_key,
                           target_fields=target_fields,
                           source_fields=source_fields,
                           update_fields=update_fields)

            drop_query = \
              """
              DROP TABLE {schema}.{temp_table};
              """.format(schema=self.azure_schema,
                         temp_table=loading_table)
            # Run the update query.
            m_hook.run([update_query, drop_query])
