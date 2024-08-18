import vertica_python  # type: ignore
import psycopg2  # type: ignore

from utils.utils  import to_flat_list, read_file_content



def run_sql(dialect: str, sql_script: str, conn_dict: dict):
    """
    Выполняет SQL-запрос на указанном диалекте базы данных.

    :param dialect: Диалект базы данных (например, 'Vertica' или 'Greenplum')
    :param sql_script: SQL-скрипт для выполнения
    :param conn_dict: Словарь с параметрами подключения
    :raise ValueError: Если указан неподдерживаемый диалект
    :return: Результат выполнения запроса
    """
    if dialect == 'Vertica':
        with vertica_python.connect(**conn_dict) as connection:
            cur = connection.cursor()
            cur.execute(sql_script)
            result = cur.fetchall()
            return result

    elif dialect == 'Greenplum':
        try:
            connection = psycopg2.connect(**conn_dict)
            cur = connection.cursor()
            cur.execute(sql_script)
            if cur.rowcount > 0:
                result = cur.fetchall()
            else:
                result = None
            return result
        except Exception as error:
            print(f"Error executing Greenplum script: {error}")
            raise
        finally:
            if cur:
                cur.close()
            if connection:
                connection.close()
    else:
        raise ValueError(f"Unsupported dialect: {dialect}")



def select_columns(dialect, cur_path, col_type,  schema, table, connection):
    dialect_path_dict = {'Vertica': f'{cur_path}/sql/work_with_meta/vertica/select_all_columns.sql',
                         'Greenplum':f'{cur_path}/sql/work_with_meta/greenplum/select_all_columns.sql'}
    col_type_dict = {'all': 'true', 'text': 'data_type ilike \'%char%\''}
    columns = run_sql(dialect, read_file_content(dialect_path_dict[dialect]).format(table=table, schema_name=schema,
                                                                               where_clause=col_type_dict[col_type]),
                          connection)
    columns_list = to_flat_list(columns)
    return columns_list
