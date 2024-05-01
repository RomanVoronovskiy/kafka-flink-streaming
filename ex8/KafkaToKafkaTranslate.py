from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.table import StreamTableEnvironment

def is_odd_number(num):
    return int(num) % 2 != 0

# Настройки для Kafka producer
producer_config = {
    "bootstrap.servers": '127.0.0.1:9092',
    "acks": "all"
}

if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    t_env = StreamTableEnvironment.create(env)

    kafka_producer = FlinkKafkaProducer(
        topic="result_topic",
        serialization_schema=SimpleStringSchema(),
        producer_config=producer_config
    )

    kafka_consumer = FlinkKafkaConsumer(
        topics='numbers_topic',
        deserialization_schema=SimpleStringSchema(),
        properties={'bootstrap.servers': '127.0.0.1:9092', 'group.id': 'test_group'}
    )
    data_stream = env.add_source(kafka_consumer)

    data_stream.filter(lambda record: is_odd_number(record)).add_sink(kafka_producer)
    env.execute()
