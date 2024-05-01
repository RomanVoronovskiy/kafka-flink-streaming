from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer

deserialization_schema = SimpleStringSchema()
kafka_consumer = FlinkKafkaConsumer(
    topics='stream_topic',
    deserialization_schema=deserialization_schema,
    properties={'bootstrap.servers': '127.0.0.1:9092', 'group.id': 'test_group'})
kafka_consumer.set_start_from_earliest()


def read_from_kafka(env):
    data_stream = env.add_source(kafka_consumer)
    data_stream.print()


if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    read_from_kafka(env)

    env.execute()
