from utils import *

class SQL():

    def __init__(self):
        """
            Initializes SQL class by creating a SQL managed resource pool and 
            retrieving pool credentials.
        """
        self._connection_pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="dofasco-dev-pool",
            pool_size=10,
            pool_reset_session=True,
            host=mysqlUrl,
            database=mysqlDb,
            user=mysqlUser,
            port=mysqlPort,
            password=mysqlPassword)

    def create(self, table_name, table_structure):
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
            connection = self._connection_pool.get_connection()
            cur = connection.cursor()
            cur.execute(cmd)
        except Exception as e:
            print(e)
            raise        
        finally:
            cur.close()
            connection.close()

    def read(self, cmd):
        """
            Retrieve the mysql datatable to pandas dataframe

            Parameters:
                cmd: <str>
                SQL Query
        """
        try:
            connection = self._connection_pool.get_connection()
            cur = connection.cursor()
            cur.execute(cmd)
        except Exception as e:
            print(e)
            raise
        else:
            response = cur.fetchall()
            return response             
        finally:
            cur.close()
            connection.close()
            
    def update(self, df, df_name, target_update=True, records_step=1000):
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

        # get the sql command
        dt_sql = "{}({})".format(df_name, ','.join(column_names))
        df_sql = "Values({}{})".format("%s," * (len(column_names) - 1), "%s")

        sql_command = "INSERT INTO {} {}".format(dt_sql, df_sql)

        if target_update:
            update_command = "ON DUPLICATE KEY UPDATE " + ", ".join([f"{x} = VALUES({x})" for x in df.columns.tolist()])
            sql_command = " ".join([sql_command, update_command])

        # push dataframe to datatable
        try:
            connection = self._connection_pool.get_connection()
            cursor = connection.cursor()

            for i in range(0, len(records), records_step):
                temp_records = records[i:i + records_step]
                cursor.executemany(sql_command, temp_records)
                connection.commit()
                print(f"{len(temp_records)} records pushed")

        except Exception as e:
            print(e)
            raise

        finally:
            cursor.close()
            connection.close()

    # TODO: Need to add statement to empty table rather than drop it
    def delete(self, table_name, params=None):
        """
            Deletes data table

            Parameters:
                table_name:   <str>
                              target table

                params:       <dict>
                              parameters specified to delete table contents
        """
        if params:
            statements = []
            for key, vals in params.items():
                if len(vals) > 1:
                    conditions = " OR ".join([f"{key}='{val}'" for val in vals])
                    conditions = f"({conditions})"
                else:
                    conditions = f"{key}='{vals}'"
                statements.append(conditions)
            statements = " AND ".join(statements)
            cmd = f"DELETE FROM {table_name} WHERE {statements}"
        else:
            cmd = f"DROP TABLE IF EXISTS {table_name};"
        
        try:
            connection = self._connection_pool.get_connection()
            cur = connection.cursor()
            cur.execute(cmd)
        except Exception as e:
            print(e)
            raise            
        finally:
            cur.close()
            connection.close()        

if __name__ == '__main__':
    sql = SQL()