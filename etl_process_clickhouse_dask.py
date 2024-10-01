import dask.dataframe as dd  # Модуль для работы с большими DataFrame, который позволяет обрабатывать данные параллельно.
import numpy as np  # Библиотека для работы с многомерными массивами данных и математическими операциями.
import pandas as pd  # Библиотека для работы с данными в формате таблиц (DataFrame), аналогичных таблицам баз данных.
from clickhouse_driver import Client  # Класс Client используется для подключения и
# взаимодействия с базой данных ClickHouse.
import matplotlib.pyplot as plt  # Модуль для построения графиков и визуализации данных.

df = pd.read_csv(r'C:\Users\Кирилл\Desktop\data.csv')  # Подключение к файлу csv через DataFrame в Pandas
print(df)
''' Чтение данных из CSV-файла с помощью pandas.read_csv() и преобразование их в DataFrame.
Путь к файлу указывается через сырую строку (r'...'), чтобы избежать проблем с экранированием символов.
print(df) выводит данные на экран для проверки. '''

# Подключение к ClickHouse
client = Client('localhost')
'''Инициализация клиента для взаимодействия с ClickHouse. Подключение происходит к серверу базы данных, 
находящемуся локально на машине'''

# SQL-запрос для создания таблицы
create_table_query = '''
CREATE TABLE IF NOT EXISTS data_table (
    id Int32,
    name String,
    surname String,                              
    age Int32,
    salary Float32
    ) ENGINE = MergeTree()
ORDER BY id                 
'''
'''SQL-запрос для создания таблицы data_table, если она не существует.
Структура таблицы включает поля: id (целочисленный), name (строка), surname (строка), age (целочисленный), и salary (дробный тип данных).
MergeTree используется как основной механизм хранения данных, обеспечивающий быстрый доступ и сортировку данных по полю id.
'''

# Преобразование данных в список кортежей для загрузки
data_to_insert = [tuple(row) for row in df.values]
'''Преобразует каждую строку DataFrame в кортеж для того, чтобы затем можно было передать эти данные в SQL-запрос в ClickHouse.
df.values возвращает двумерный массив данных DataFrame, который затем преобразуется в список кортежей.'''

# SQL-запрос для вставки данных
insert_query = 'INSERT INTO data_table (id, name, surname, age, salary) VALUES'
'''insert_query содержит SQL-команду для вставки данных в таблицу data_table.
client.execute() выполняет SQL-запрос, вставляя данные в ClickHouse.'''

# Выполнение запроса на вставку данных
client.execute(insert_query, data_to_insert)

# Проверка, что данные были загружены
select_query = 'SELECT * FROM data_table'
result = client.execute(select_query)
print(result)
'''SQL-запрос для выбора всех данных из таблицы data_table.
client.execute() возвращает результат запроса, который затем выводится на экран с помощью print() для проверки того, что данные были успешно загружены.'''

# Создаём большой DataFrame с миллионами строк
n = 10 ** 6  # 1 million string
df = pd.DataFrame({
    'id': np.arange(1, n + 1),
    'name': np.random.choice(['John', 'Kate', 'Anna', 'Mike', 'Valentina'], n),
    'surname': np.random.choice(['Malkovich', 'Darison', 'Petrovich', 'Adriano', 'Tereshkova'], n),
    'age': np.random.randint(20, 60, size=n),
    'salary': np.random.uniform(30000, 120000, size=n)
})
'''Создаёт DataFrame с 1 миллионом строк:
id: Последовательные числа от 1 до миллиона.
name: Случайные имена из списка.
surname: Случайные фамилии из списка.
age: Случайные целые числа в диапазоне от 20 до 60.
salary: Случайные зарплаты в диапазоне от 30,000 до 120,000.'''

# Преобразуем Pandas DataFrame в Dask DataFrame
ddf = dd.from_pandas(df, npartitions=10)
'''Преобразует Pandas DataFrame в Dask DataFrame с 10 частями (npartitions=10) для параллельной обработки.'''

# Показываем первые пять строк. Можно изменить на любое число
print(ddf.head())

'''ddf.head() выводит первые 5 строк DataFrame для проверки.'''


# Функция для загрузки одной части данных

def insert_to_clickhouse(partition):
    client = Client(host='localhost')
    data_to_insert = [tuple(row) for row in partition.values]
    insert_query = 'INSERT INTO data_table (id, name, surname, age, salary) VALUES'
    client.execute(insert_query, data_to_insert)


'''Определяет функцию, которая принимает одну часть данных (раздел DataFrame) и загружает её в базу данных ClickHouse.
Подключение создается внутри функции для каждой части данных.
Данные из части преобразуются в список кортежей, затем вставляются в таблицу ClickHouse.'''

# Применяем функцию ко всем частям DataFrame
meta = ddf._meta
ddf.map_partitions(insert_to_clickhouse, meta=meta).compute()
'''ddf.map_partitions() применяет функцию insert_to_clickhouse ко всем частям Dask DataFrame.
compute() выполняет расчёт и завершает все операции по загрузке данных в ClickHouse.
'''

# Запрос для получения возрастов
select_query = 'SELECT age FROM my_table'
ages = client.execute(select_query)

# Преобразуем результат в список
age_list = [age[0] for age in ages]
plt.figure(figsize=(10, 6))
plt.hist(age_list, bins=20, color='blue', alpha=0.7)
plt.title('Распределение возрастов')
plt.xlabel('Возраст')
plt.ylabel('Количество')

# Показываем график
plt.grid(axis='y', alpha=0.75)
plt.show()

'''Выполняет SQL-запрос для получения возрастов из таблицы и преобразует результат в список.
Использует plt.hist() для построения гистограммы распределения возрастов.
Настраивает внешний вид графика (цвет, размеры, заголовок, сетку) и отображает его с помощью plt.show().'''

select_query = 'SELECT salary FROM data_table'
salaries = client.execute(select_query)
salary_list = [salary[0] for salary in salaries]

plt.figure(figsize=(10, 6))
plt.hist(salary_list, bins=30, color='green', alpha=0.7)
plt.title('Распределение зарплат')
plt.xlabel('Зарплата')
plt.ylabel('Количество')
plt.grid(axis='y', alpha=0.75)
plt.show()

'''Выполняет SQL-запрос для получения зарплат из таблицы, а затем строит гистограмму распределения зарплат с помощью plt.hist().
График настраивается по аналогии с предыдущим шагом.'''
