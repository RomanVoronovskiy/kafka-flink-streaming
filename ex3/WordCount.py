from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes, expressions as expr
from pyflink.table.udf import udf

def split_words(line):
    return len(line.lower().split())

if __name__ == "__main__":
    # Создаем стриминговое окружение PyFlink
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    t_env = StreamTableEnvironment.create(env)

    # Чтение данных из файла с указанием кодировки ISO-8859-1
    t_env.execute_sql("""
        CREATE TABLE text_source (
            line STRING
        ) WITH (
            'connector' = 'filesystem', 
            'format' = 'csv',
            'path' = '\\data\\A.csv' ,
             'csv.field-delimiter' = '\n'
        )
    """)

    # Определение таблицы для записи результатов обработки данных
    t_env.execute_sql("""
        CREATE TABLE result_sink (
            word_count BIGINT
        ) WITH (
            'connector' = 'filesystem', 
            'path' = '\\output' ,
            'format' = 'csv'
        )
    """)

    # Регистрируем пользовательскую функцию для разделения строки на слова
    t_env.create_temporary_system_function(
        "split_words",
        udf(split_words, input_types=[DataTypes.STRING()], result_type=DataTypes.BIGINT())
    )

    # Разделение строк на слова и подсчет слов
    result_table = t_env.from_path("text_source") \
        .select(expr.call("split_words", expr.col("line")).alias("word_count"))

    # Запись данных из result_table в таблицу result_sink
    result_table.execute_insert("result_sink").wait()

    # Вывод данных из таблицы result_sink в консоль
    t_env.execute_sql("SELECT * FROM result_sink").print()