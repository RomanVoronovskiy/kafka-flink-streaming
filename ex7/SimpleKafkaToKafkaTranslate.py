from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.table import StreamTableEnvironment

if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    t_env = StreamTableEnvironment.create(env)

    kafka_consumer = FlinkKafkaConsumer(
        topics='stream_topic',
        deserialization_schema=SimpleStringSchema(),
        properties={'bootstrap.servers': '127.0.0.1:9092', 'group.id': 'test_group'}
    )
    kafka_consumer.set_start_from_earliest()

    data_stream = env.add_source(kafka_consumer)
    data_stream = data_stream.filter(lambda x: len(x) > 5)
    table = t_env.from_data_stream(data_stream)

    t_env.execute_sql("""
        CREATE TABLE result_sink (
            message STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'result_topic',
            'properties.bootstrap.servers' = '127.0.0.1:9092',
            'format' = 'json'
        )
    """)

    table.execute_insert("result_sink").wait()