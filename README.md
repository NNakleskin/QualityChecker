# QualityChecker
Позволяет проводить проверки таблиц в вертике. на 25.10.2023
1. Дубли по ключам
2. Полностью пустые столбцы (NULL или '')
3. Текстовые поля длина которых достигла максимума.
4. Наличие кривых символов (не utf-8).
5. Какая макс. tech_load_ts в ODS.
6. Проверка корректности инкремента (тестируется)
7. Статистика длин текстовых полей. varchar самого большого значения и максимальный.
8. Статистика самых часто встречающихся значений в поле и их доля от всех.
9. Сегментация
10. Количество строк в STG
11. Количество строк в ODS

Устанвока библиотек
1. pip install vertica-python, pandas, openpyxl(на кспд можно попробовать установить через командную строку, запустив от имени администратора)
2. Перенести и распаковать архив в удобное месте.
3. Открыть anaconda powershell.
4. Перейти в папку с файлом QualityChecker\QualityCheker\main.py.
5. Запустить команду. (python .\main.py)
6. В папке report смотреть отчеты.
