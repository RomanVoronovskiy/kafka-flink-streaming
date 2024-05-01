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

    t_env.execute_sql("""
        CREATE TABLE result_sink (
            max_word_count BIGINT
        ) WITH (
            'connector' = 'filesystem', 
            'format' = 'csv',
            'path' = '\\output',
            'sink.partition-commit.trigger' = 'process-time'
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

    # Нахождение максимального значения в столбце word_count таблицы result_sink
    max_word_count = t_env.from_path("result_sink") \
        .select(expr.col("max_word_count").max.alias("max_word_count"))

    # Преобразование результатов в Pandas DataFrame
    df = max_word_count.to_pandas()

    # Преобразование DataFrame в список строк
    max_word_count_list = df["max_word_count"].tolist()

    # Вывод данных из таблицы result_sink в консоль
    t_env.execute_sql("SELECT max_word_count FROM result_sink").print()
    # Распечатка максимального значения
    for row in max_word_count_list:
        print("Максимальное количество слов в строке: "+str(row))


