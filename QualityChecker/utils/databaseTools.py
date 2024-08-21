import vertica_python  # type: ignore
import psycopg2  # type: ignore
import logging

from utils.utils  import to_flat_list, read_file_content



def run_sql(dialect: str, sql_script: str, conn_dict: dict):
    """
    Выполняет SQL-запрос в базе данных, используя указанный диалект и параметры подключения.

    Функция подключается к базе данных, используя предоставленные параметры подключения,
    и выполняет SQL-запрос. В зависимости от диалекта базы данных (например, 'Vertica' или 'Greenplum'),
    используется соответствующая библиотека для работы с базой данных.

    Параметры:
    ----------
    dialect : str
        Тип базы данных, для которой нужно выполнить запрос (например, 'Vertica', 'Greenplum').
    sql_script : str
        SQL-запрос, который необходимо выполнить.
    conn_dict : dict
        Словарь с параметрами подключения к базе данных.

    Возвращает:
    -----------
    list или None
        Возвращает результат выполнения запроса в виде списка записей, если запрос вернул данные.
        Если запрос не вернул данных или произошла ошибка, возвращается None.

    Исключения:
    -----------
    ValueError
        Выбрасывается, если указан неподдерживаемый диалект базы данных.
    Exception
        Выбрасывается, если при выполнении запроса в базе данных Greenplum произошла ошибка.
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
            logging.INFO((f"Error executing Greenplum script: {error}"))
            raise
        finally:
            if cur:
                cur.close()
            if connection:
                connection.close()
    else:
        raise ValueError(f"Unsupported dialect: {dialect}")



def select_columns(dialect, cur_path, col_type,  schema, table, connection):
    """
    Получает список столбцов из заданной таблицы базы данных в зависимости от типа столбцов и базы данных.

    Эта функция читает SQL-запрос из файла, соответствующего указанному диалекту базы данных, 
    и выполняет его для получения списка столбцов из указанной таблицы в базе данных.

    Параметры:
    ----------
    dialect : str
        Тип базы данных, для которой нужно выполнить запрос (например, 'Vertica', 'Greenplum').
    cur_path : str
        Текущий путь, где находятся SQL-файлы.
    col_type : str
        Тип столбцов для выборки. Возможные значения: 'all' (все столбцы) или 'text' (текстовые столбцы).
    schema : str
        Схема базы данных, содержащая таблицу.
    table : str
        Имя таблицы, из которой необходимо выбрать столбцы.
    connection : object
        Соединение с базой данных для выполнения запроса.

    Возвращает:
    -----------
    list
        Список столбцов, найденных в указанной таблице, приведенный к плоскому списку.
    """
    dialect_path_dict = {'Vertica': f'{cur_path}/sql/work_with_meta/vertica/select_all_columns.sql',
                         'Greenplum':f'{cur_path}/sql/work_with_meta/greenplum/select_all_columns.sql'}
    col_type_dict = {'all': 'true', 'text': 'data_type ilike \'%char%\''}
    columns = run_sql(dialect, read_file_content(dialect_path_dict[dialect]).format(table=table, schema_name=schema,
                                                                               where_clause=col_type_dict[col_type]),
                          connection)
    columns_list = to_flat_list(columns)
    return columns_list
