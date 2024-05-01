from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.table import StreamTableEnvironment

def is_prime_number(num_str):
    try:
        num = int(num_str)
    except ValueError:
        return False

    if num <= 1:
        return False
    for i in range(2, int(num ** 0.5) + 1):
        if num % i == 0:
            return False
    return True

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

    data_stream.filter(lambda record: is_prime_number(record)).add_sink(kafka_producer)
    env.execute()
