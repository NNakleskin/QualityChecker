"""
QualityChecker - утилита для автоматизированной проверки таблиц ODS и STG на стандартные виды ошибок
"""
import logging
import os
import time

from conf import vertica_conn_dict, greenplum_conn_dict
from utils.databaseTools import run_sql
from utils.utils import read_file_content
from DataQuality import DataQuality

# -------------------------------------------------------------
# Доступные проверки

# 1. Дубли по ключам
# 2. Полностью пустые столбцы (NULL или '')
# 3. Текстовые поля длина которых достигла максимума
# 4. Наличие кривых символов (не utf-8)
# 5. Какая макс. tech_load_ts в ODS
# 6. Проверка корректности инкремента (тестируется, только для Vertica)
# 7. Статистика самых часто встречающихся значений в поле и их доля от всех.
# 8. Статистика длин текстовых полей. varchar самого большого значения и максимальный.
# 9. Сегментация
# 10. Количество строк в STG.
# 11. Количество строк в ODS.
# 12. Кол-во уникальных ключей без tech_load_ts
# 13. Дубли по ключу в stg
# 14. Какая макс. tech_load_ts в ODS в STG
# -------------------------------------------------------------


def get_tables(path: str):
    path = os.path.dirname(os.path.abspath(__file__))
    sql_query = read_file_content(f'{path}/get_tables_sql_query.sql')

    obj_list = run_sql(
        dialect,
        sql_query,
        connection)
    return obj_list


def main():
    path = os.path.dirname(os.path.abspath(__file__))

    obj_list = get_tables(path=path)

    empty_tables = []
    b = time.strftime("%Y-%m-%d_%H-%M")
    report_name = f'{ENV}_report'
    print(obj_list)
    for obj in obj_list:
        table_obj = DataQuality(obj)
        table_obj.checks = checks_list
        print(f'Начало проверки таблицы  {table_obj.schema}.{table_obj.table}  select analyze_statistics(\'{table_obj.schema}.{table_obj.table}\')')
        print(time.strftime("%Y-%m-%d %H:%M"))
        if dialect == 'Vertica':
            run_sql(dialect, f'select analyze_statistics(\'{table_obj.schema}.{table_obj.table}\')', connection)
        elif dialect == 'Greenplum':
            run_sql(dialect, f'ANALYZE {table_obj.schema}.{table_obj.table};', connection)
        if bool(run_sql(dialect, f'select 1 from {table_obj.schema}.{table_obj.table} limit 1', connection)):
            table_obj.execute_checks(dialect, path, connection)
            table_obj.create_xlsx(path, report_name, b)
        else:
            logging.warning(f'Таблица {table_obj.schema}.{table_obj.table} пустая')
            empty_tables.append(f'{table_obj.schema}.{table_obj.table}')
            print(empty_tables)

    print(f'Check Results in `QualityChecker/reports/{report_name}_{b}.xlsx')
    print(empty_tables)
    print(b)
    print(time.strftime("%Y-%m-%d_%H-%M"))


if __name__ == '__main__':
    global ENV
    global connection
    global dialect
    global checks_list
    ENV = 'DEV'
    connection = vertica_conn_dict[ENV]
    dialect = ''
    checks_list = []
    print("Какое окружение?\n",
          "[1] DEV\n",
          "[2] TEST\n",
          "[3] PROD\n",)
    env_no = int(input())
    if env_no == 1:
        ENV = 'DEV'
    elif env_no == 2:
        ENV = 'TEST'
    elif env_no == 3:
        ENV = 'PROD'
    while(True):
        print("Какой диалект нужен?\n",
            "[1] - Vertica\n",
            "[2] - Greenplum\n")
        response = int(input())
        if response == 1:
            dialect = 'Vertica'
            connection = vertica_conn_dict[ENV]
            break
        elif response == 2:
            dialect = 'Greenplum'
            connection = greenplum_conn_dict[ENV]
            break
        else:
            print("Error: Unexpected value\n")
    print("Перечислите через запятую номера проверок\n",
          "0. Все\n"
          "1. Дубли по ключам\n",
          "2. Полностью пустые столбцы (NULL или '')\n",
          "3. Текстовые поля длина которых достигла максимума\n",
          "4. Наличие кривых символов (не utf-8)\n",
          "5. Какая макс. tech_load_ts в ODS\n",
          "6. Проверка корректности инкремента (тестируется)\n",
          "7. Статистика самых часто встречающихся значений в поле и их доля от всех.\n",
          "8. Статистика длин текстовых полей. varchar самого большого значения и максимальный.\n",
          "9. Сегментация\n",
          "10. Количество строк в STG.\n",
          "11. Количество строк в ODS.\n",
          "12. Кол-во уникальных ключей без tech_load_ts\n",
          "13. Дубли по ключу в stg\n",
          "14. Какая макс. tech_load_ts в ODS в STG\n")
    checks_list = list(map(int, input().split(',')))
    if 0 in checks_list:
        checks_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]
    print(ENV, dialect, checks_list)
    main()
