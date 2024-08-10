"""

"""
import logging
import os
import time

from conf import vertica_conn_dict
from utils.databaseTools import run_sql
from utils.utils import read_file_content
from DataQuality import DataQuality


#TODO сделать консольный интерфейс
ENV = 'DEV'
connection = vertica_conn_dict[ENV]

# Ниже перечислить номера проверок
dialect = 'Vertica'
# checks = [10]
# 1. Дубли по ключам
# 2. Полностью пустые столбцы (NULL или '')
# 3. Текстовые поля длина которых достигла максимума
# 4. Наличие кривых символов (не utf-8)
# 5. Какая макс. tech_load_ts в ODS
# 6. Проверка корректности инкремента (тестируется)
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
        print(f'Начало проверки таблицы  {table_obj.schema}.{table_obj.table}  select analyze_statistics(\'{table_obj.schema}.{table_obj.table}\')')
        print(time.strftime("%Y-%m-%d %H:%M"))
        run_sql(dialect, f'select analyze_statistics(\'{table_obj.schema}.{table_obj.table}\')', connection)
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
    print('Погнали')
    main()
