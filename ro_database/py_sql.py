# PostgreSQL & SQL Alchemy Library imports
import os
import json
import psycopg2
import pandas as pd

from utils import *
from sqlalchemy import create_engine

class SQL(): 

    def __init__(self):
        """
            Initializes SQL class by creating a SQL managed resource pool and 
            retrieving pool credentials.
        """

        if 'VCAP_SERVICES' in os.environ:
            vcap = json.loads(os.getenv('VCAP_SERVICES'))

            # PostgreSQL database
            if 'databases-for-postgresql' in vcap:
                self._pgsqlCreds = vcap['databases-for-postgresql'][0]["credentials"]["connection"]["postgres"]
                self._pgsqlHost = self._pgsqlCreds["hosts"][0]["hostname"]
                self._pgsqlPort = self._pgsqlCreds["hosts"][0]["port"]
                self._pgsqlUser = self._pgsqlCreds["authentication"]["username"]
                self._pgsqlPass = self._pgsqlCreds["authentication"]["password"]
                self._pgsqlDbname = self._pgsqlCreds["database"]
                self._pgsqlAlcehmy = self._pgsqlCreds["composed"][0]
            else:
                raise MissingCreds("SQL creds not fouund in OS Environment!")
        elif os.path.isfile('/../../../app/vcap_services.json'):
            with open('/../../../app/vcap_services.json') as f:
                vcap = json.load(f)
                print('Found local VCAP_SERVICES')
                
            # PostgreSQL database
            if 'databases-for-postgresql' in vcap:
                self._pgsqlCreds = vcap['databases-for-postgresql'][0]["credentials"]["connection"]["postgres"]
                self._pgsqlHost = self._pgsqlCreds["hosts"][0]["hostname"]
                self._pgsqlPort = self._pgsqlCreds["hosts"][0]["port"]
                self._pgsqlUser = self._pgsqlCreds["authentication"]["username"]
                self._pgsqlPass = self._pgsqlCreds["authentication"]["password"]
                self._pgsqlDbname = self._pgsqlCreds["database"]
                self._pgsqlAlcehmy = self._pgsqlCreds["composed"][0]
            else:
                raise MissingCreds("VCAP_SERVICES Not found in OS Environment!")
        else:
            raise MissingCreds("VCAP_SERVICES Not found in OS Environment!")

        self._conn_string = "host="+self._pgsqlHost+ \
                            " port="+str(self._pgsqlPort)+ \
                            " dbname="+self._pgsqlDbname+\
                            " user="+self._pgsqlUser+\
                            " password="+self._pgsqlPass

        
        self._alchemy_engine = create_engine(self._pgsqlAlcehmy, connect_args={'sslrootcert': os.getenv('POSTGRESQL_ROOT_CRT')})
 
    def create(self, table_name: str, table_structure: list):
        """
            Creates new SQL table

            Parameters:
                table_name:       <str>
                                  name of data table
                
                table_structure:  <list>
                                  list of strings where each entry contains:
                                  1. column name
                                  2. column type & limits
                                  3. whether column allows null values or not
        """
        primary_key = table_structure[-1]
        table = ", ".join(table_structure[:-1])
        cmd = f"CREATE TABLE {table_name} ({table}, PRIMARY KEY ({primary_key}));"
        try:
            connection = psycopg2.connect(self._conn_string, sslrootcert="root.crt")
            cur = connection.cursor()
            cur.execute(cmd)
        except Exception as e:
            print(e)
            raise        
        finally:
            cur.close()
            connection.close()

    def read(self, table_name: str, schema: str, columns: list=None):
        """
            Retrieve the sql datatable to pandas dataframe

            Parameters:
                table_name: sql table name
                schema:     sql schema name
                column:     columns to retrieve from database
                
            Return: 
                df:         sql table
        """

        try:
            conn = self._alchemy_engine.connect()
            if columns:
                df = pd.read_sql_table(table_name=table_name, columns=columns, schema=schema, con=conn)
            else:
                df = pd.read_sql_table(table_name=table_name, schema=schema, con=conn)
        except Exception as e:
            print(e)
            raise
        else:
            conn.close()
            return df
            
    def update(self, df, df_name: str, schema: str, target_update: bool = None, pmkey: str = None):
        """
            Retrieve the mysql datatable to pandas dataframe
            Parameters:
                df:           <pandas dataframe>
                              dataframe to be uploaded or updated on SQL DB
                df_name:       <str>
                               data table name
                target_update: <bool>
                                update columns of interest
                
                records_step: <int>
                              number of rows to update/upload per bulk
        """
        column_names = df.columns
        records = [tuple(row) for _, row in df.iterrows()]

        # print(records)

        # get the sql command
        df_name = f"{schema}.{df_name}"
        dt_sql = "{} ({})".format(df_name, ','.join(column_names))
        df_sql = "VALUES({}{})".format("%s," * (len(column_names) - 1), "%s")
        
        sql_command = f"""INSERT INTO {df_name} {df_sql}"""
        
        if target_update:
            update_command = f"ON CONFLICT ({pmkey}) DO UPDATE SET " + ", ".join([f"{x}=excluded.{x}" for x in df.columns.tolist()])
            sql_command = " ".join([sql_command, update_command])

            print(sql_command)
            
        try:
            connection = psycopg2.connect(self._conn_string)
            cur = connection.cursor()
            connection.autocommit = True
            psycopg2.extras.execute_batch(cur, sql_command, records, page_size = len(records))
        except Exception as e:
            connection.rollback()
            print(e)
            raise
        finally:
            cur.close()
            connection.close()

    # TODO: Need to add statement to empty table rather than drop it
    # def delete(self, table_name, params=None):
    #     """
    #         Deletes data table

    #         Parameters:
    #             table_name:   <str>
    #                           target table

    #             params:       <dict>
    #                           parameters specified to delete table contents
    #     """
    #     if params:
    #         statements = []
    #         for key, vals in params.items():
    #             if len(vals) > 1:
    #                 conditions = " OR ".join([f"{key}='{val}'" for val in vals])
    #                 conditions = f"({conditions})"
    #             else:
    #                 conditions = f"{key}='{vals}'"
    #             statements.append(conditions)
    #         statements = " AND ".join(statements)
    #         cmd = f"DELETE FROM {table_name} WHERE {statements}"
    #     else:
    #         cmd = f"DROP TABLE IF EXISTS {table_name};"
        
    #     try:
    #         connection = psycopg2.connect(self._conn_string)
    #         cur = connection.cursor()
    #         cur.execute(cmd)
    #     except Exception as e:
    #         print(e)
    #         raise            
    #     finally:
    #         cur.close()
    #         connection.close()

if __name__ == '__main__':
    py_sql = SQL()
