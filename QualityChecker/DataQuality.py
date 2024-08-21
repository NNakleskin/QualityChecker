import traceback
import pandas  # type: ignore
import os
import logging

from checks import max_length, check_pk_doubles, not_utf8, check_insert_new_rows, \
    check_most_consistent_value, check_columns_length_statistics, check_max_tech_load_ts, check_row_count, \
    check_null_fields, check_segmentation, check_bussines_key_counts

from utils.utils import to_flat_list
from utils.databaseTools import select_columns


class DataQuality:
    checks = []

    schema = str
    table = str
    all_columns_df = []
    unique = []
    check_not_null_fields_result_df = []
    check_stat_value_result_df = []

    schema_df = []
    table_df = []
    col_df = []
    col_schema_df = []
    col_table_df = []

    check_pk_doubles_df = []
    check_max_tech_load_ts_df = []
    count_not_utf8_cols_df = []
    count_null_cols_df = []
    count_max_length_df = []
    check_row_count_ods_df = []
    check_row_count_stg_df = []
    check_segmentation_df = []
    check_bussines_key_counts_df = []
    check_stg_pk_doubles_df = []
    check_max_tech_load_ts_stg_df = []

    max_length_df = []
    not_utf8_df = []
    stat_most_cons_val_df = []
    check_columns_length_statistics_result_df = []
    not_null_df = []

    def __init__(self, obj):
        self.schema = obj[0]
        self.table = obj[1]

    def execute_checks(self, dialect: str, path: str, connection: dict) -> None:
        """
        Выполняет набор проверок данных для заданной таблицы в базе данных и сохраняет результаты.

        Функция выполняет несколько предопределенных проверок на данных таблицы, такие как проверка дублей по ключу,
        подсчет пустых полей, проверка максимальной длины текстовых полей, наличие не-UTF-8 символов и другие.
        Результаты проверок сохраняются в соответствующие атрибуты объекта.

        Параметры:
        ----------
        dialect : str
            Диалект базы данных (например, 'Vertica', 'Greenplum'), который используется для выполнения запросов.
        path : str
            Путь к директории, содержащей SQL-запросы и другие необходимые файлы.
        connection : object
            Подключение к базе данных, используемое для выполнения SQL-запросов.

        Возвращает:
        -----------
        None
            Функция не возвращает значений, результаты проверок сохраняются в атрибутах объекта.

        Примечания:
        ----------
        - В зависимости от набора проверок, определенного в self.checks, функция выполняет разные операции.
        - Результаты проверок сохраняются в виде списков в атрибутах объекта, таких как `self.col_df`, `self.not_null_df`, `self.max_length_df` и других.
        - Некоторые проверки специфичны для определенных типов столбцов (например, текстовых).
        - При возникновении ошибок в процессе проверки корректности инкремента (check_insert_new_rows) будет выведено сообщение об ошибке.
        """
        self.table_df.append(self.table)
        self.schema_df.append(self.schema)
        all_columns_list = select_columns(dialect, path, 'all', self.schema, self.table, connection)
        text_columns_list = select_columns(dialect, path, 'text', self.schema, self.table, connection)
        for col in all_columns_list:
            self.col_table_df.append(self.table)
            self.col_schema_df.append(self.schema)
            self.col_df.append(col)
        if 1 in self.checks:
            logging.info('1. Проверка дублей по ключу')
            self.check_pk_doubles_df.append(check_pk_doubles(dialect, self.schema, self.table, connection))

        if 2 in self.checks:
            logging.info('2. Полностью пустые')
            cnt = 0
            for col in all_columns_list:
                result = to_flat_list(check_null_fields(dialect, self.schema, self.table, col, connection))
                self.not_null_df.append(result[0])
                if result[0] == 1:
                    cnt = cnt + 1
            self.count_null_cols_df.append(cnt)

        if 3 in self.checks:
            logging.info('3. Проверка максимальных длин полей')
            cnt = 0
            for col in all_columns_list:
                if col in text_columns_list:
                    result = to_flat_list(max_length(dialect, self.schema, self.table, col, connection))
                    self.max_length_df.append(result[0])
                    if result[0] == 1:
                        cnt = cnt + 1
                else:
                    self.max_length_df.append('-')
            self.count_max_length_df.append(cnt)

        if 4 in self.checks:
            logging.info('4. Проверка есть ли UTF-8 символы')
            cnt = 0
            for col in all_columns_list:
                result = to_flat_list(not_utf8(dialect, self.schema, self.table, col, connection))
                if col in text_columns_list:
                    self.not_utf8_df.append(result[0])
                else:
                    self.not_utf8_df.append('-')
                if result[0] == 1:
                    cnt = cnt + 1
            self.count_not_utf8_cols_df.append(cnt)

        if 5 in self.checks:
            logging.info('5. Максимальная tech_load_ts ODS')
            self.check_max_tech_load_ts_df.append(check_max_tech_load_ts(dialect, self.schema, self.table, connection)[0])

        if 6 in self.checks:
            logging.info('6.DRAFT Проверка корректности инкремента')
            try:
                check_insert_new_rows(dialect, self.schema, self.table, connection)
            except:
                logging.info('Ошибка:\n', traceback.format_exc())
        logging.info(self.schema)
        if 7 in self.checks:
            logging.info('7. Самое часто встречающееся значание')
            for col in all_columns_list:
                self.stat_most_cons_val_df.append(
                    to_flat_list(check_most_consistent_value(dialect, self.schema, self.table, col, connection))[0])

        if 8 in self.checks:
            logging.info('8. Статистика длины текстовых полей')
            for col in all_columns_list:
                if col in text_columns_list:
                    self.check_columns_length_statistics_result_df.append(
                        to_flat_list(check_columns_length_statistics(dialect, self.schema, self.table, col, connection))[0])
                else:
                    self.check_columns_length_statistics_result_df.append('-')
        if 9 in self.checks:
            logging.info('9. Сегментация')
            self.check_segmentation_df.append(check_segmentation(dialect, self.schema, self.table, connection))

        if 10 in self.checks:
            logging.info('10. Количество STG')
            stg_schema = self.schema.replace('ODS_', 'STG_')
            self.check_row_count_stg_df.append(check_row_count(dialect, stg_schema, self.table, connection)[0])

        if 11 in self.checks:
            logging.info('11. Количество ODS')
            self.check_row_count_ods_df.append(check_row_count(dialect, self.schema, self.table, connection)[0])

        if 12 in self.checks:
            logging.info('12. Количество бизнес ключей в ods')
            self.check_bussines_key_counts_df.append(check_bussines_key_counts(dialect, self.schema, self.table, connection))

        if 13 in self.checks:
            logging.info('13. Дубли по ключу в stg')
            stg_schema = self.schema.replace('ODS_', 'STG_')

            self.check_stg_pk_doubles_df.append(check_pk_doubles(dialect, stg_schema, self.table, connection))

        if 14 in self.checks:
            logging.info('14. Максимальная tech_load_ts STG')
            stg_schema = self.schema.replace('ODS_', 'STG_')
            self.check_max_tech_load_ts_stg_df.append(check_max_tech_load_ts(dialect, stg_schema, self.table, connection)[0])


    def create_xlsx(self, path: str, report_name: str, b: str) -> None:
        """
        Создает или обновляет отчет в формате Excel с двумя листами на основе текущих данных и проверок.

        Функция генерирует два листа Excel, используя данные, которые зависят от списка проверок, 
        определенного в объекте. Если отчет уже существует, данные будут добавлены к существующим, 
        иначе создается новый файл.

        Параметры:
        ----------
        path : str
            Путь к директории, где будет сохранен файл отчета.
        report_name : str
            Имя файла отчета (без расширения).
        b : str
            Дополнительный параметр, используемый для уникализации имени файла отчета.

        Возвращает:
        -----------
        None
            Функция не возвращает значений, а создает или обновляет файл Excel.

        Примечания:
        ----------
        - Функция проверяет наличие существующего файла по указанному пути. Если файл существует, 
        новые данные добавляются к текущим листам "General" и "Detail". 
        - Если файл не существует, создается новый файл с двумя листами на основе текущих данных.
        """
        list1_all_options_dict = {
                'ods_pk_doubles': self.check_pk_doubles_df,
                'stg_pk_doubles': self.check_stg_pk_doubles_df,
                'ods_row_count': self.check_row_count_ods_df,
                'stg_row_count': self.check_row_count_stg_df,
                'bk_counts': self.check_bussines_key_counts_df,
                'max_ts_ods': self.check_max_tech_load_ts_df,
                'max_ts_stg': self.check_max_tech_load_ts_stg_df,
                'ods_null_fields': self.count_null_cols_df,
                'ods_max_length': self.count_max_length_df,
                'ods_not_utf8': self.count_not_utf8_cols_df,
                'segmentation': self.check_segmentation_df,
            }

        list2_all_options_dict = {
            'null_cols': self.not_null_df,
            'not_utf8': self.not_utf8_df,
            'max_length': self.max_length_df,
            'Consist': self.stat_most_cons_val_df,
            'length stat': self.check_columns_length_statistics_result_df
        }

        number_of_check_in_checks_variable_1 = {
            'ods_pk_doubles': 1,
            'stg_pk_doubles': 13,
            'ods_row_count': 11,
            'stg_row_count': 10,
            'bk_counts': 12,
            'max_ts_ods': 5,
            'max_ts_stg': 14,
            'ods_null_fields': 2,
            'ods_max_length': 3,
            'ods_not_utf8': 4,
            'segmentation': 9,
        }

        number_of_check_in_checks_variable_2 = {
            'null_cols': 2,
            'not_utf8': 4,
            'max_length': 3,
            'Consist': 7,
            'length stat': 8
        }

        list1_df_components_dict = {}
        list1_df_components_dict['schema'] = self.schema_df
        list1_df_components_dict['table'] = self.table_df
        list1_df_components_dict.update({key: list1_all_options_dict[key] for key in list1_all_options_dict if
                                            number_of_check_in_checks_variable_1.get(key, None) in self.checks})

        list2_df_components_dict = {}
        list2_df_components_dict['schema'] = self.col_schema_df
        list2_df_components_dict['table'] = self.col_table_df
        list2_df_components_dict['column'] = self.col_df
        list2_df_components_dict.update({key: list2_all_options_dict[key] for key in list2_all_options_dict if
                                            number_of_check_in_checks_variable_2.get(key, None) in self.checks})

        df_list1 = pandas.DataFrame(list1_df_components_dict)
        df_list2 = pandas.DataFrame(list2_df_components_dict)

        if os.path.isfile(f'{path}/reports/{report_name}_{b}.xlsx'):
            writer_sheet1 = pandas.read_excel(f'{path}/reports/{report_name}_{b}.xlsx', header=0, sheet_name='General')
            writer_sheet2 = pandas.read_excel(f'{path}/reports/{report_name}_{b}.xlsx', header=0, sheet_name='Detail')
            frame1 = [writer_sheet1, df_list1]
            df_result1 = pandas.concat(frame1)

            frame2 = [writer_sheet2, df_list2]
            df_result2 = pandas.concat(frame2)

            writer = pandas.ExcelWriter(f'{path}/reports/{report_name}_{b}.xlsx')
            df_result1.to_excel(writer, sheet_name='General', index=False)
            df_result2.to_excel(writer, sheet_name='Detail', index=False)
            writer.close()

        else:
            writer = pandas.ExcelWriter(f'{path}/reports/{report_name}_{b}.xlsx')

            df_list1.to_excel(writer, sheet_name='General', index=False)
            df_list2.to_excel(writer, sheet_name='Detail', index=False)
            writer.close()