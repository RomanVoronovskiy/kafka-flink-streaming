from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(env)

    # Чтение данных из CSV-файла и создание таблицы
    t_env.execute_sql("""
        CREATE TABLE MyTable (
            name STRING,
            age INT
        ) WITH (
            'connector' = 'filesystem',
            'format' = 'csv',
            'path' = '\\data\\ex1_data.csv'
        )
    """)

    # Определение таблицы для записи результатов обработки данных
    t_env.execute_sql("""
        CREATE TABLE result_sink (
            name STRING,
            age INT,
            city STRING
        ) WITH (
            'connector' = 'filesystem',
            'path' = '\\output',
            'format' = 'csv'
        )
    """)

    # Написание SQL-запроса для обработки данных (фильтрация по возрасту)
    t_env.execute_sql("""
        INSERT INTO result_sink
        SELECT name, age , 'Moscow' AS city
        FROM MyTable
        WHERE age >= 20
    """)

    # Вывод данных из таблицы result_sink в консоль
    t_env.execute_sql("SELECT * FROM result_sink").print()
