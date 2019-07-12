import argparse
import sys
from time import sleep
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic


def create_job_queue_topic(topic_name: str, number_of_partitions: int, broker: str, stop_topic: str):
    admin_client = AdminClient({'bootstrap.servers': [broker]})

    kafka_up = False
    md = ""
    while not kafka_up:
        try:
            print("Checking if Kafka is up...")
            md = admin_client.list_topics(timeout=10)
        except:
            continue
        kafka_up = True

    print("Kafka is up!")
    print(md)

    fs = admin_client.create_topics([NewTopic(topic_name, num_partitions=number_of_partitions, replication_factor=1),
                                     NewTopic(stop_topic, num_partitions=1, replication_factor=1)])

    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print("Topic {} created".format(topic))
        except Exception as e:
            print("Failed to create topic {}: {}".format(topic, e))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", help="Address for the Kafka broker", default="kafka:29092")
    parser.add_argument("--topic", help="Topic to publish messages to", default="job_queue")
    parser.add_argument("--number-of-jobs", default=100)
    parser.add_argument("--consumers-in-pool", default=10)
    args = parser.parse_args()

    stop_topic = 'kill_all_consumers'
    create_job_queue_topic(args.topic, args.consumers_in_pool, args.broker, stop_topic)

    conf = {'bootstrap.servers': args.broker}
    p = Producer(**conf)

    job_length = 5
    for message_id in range(args.number_of_jobs):
        p.produce(args.topic, f'{{"id": "{message_id}", "job_length": "{job_length}"}}')
        p.poll(timeout=5)

    sleep(10)
    p.produce(stop_topic, 'STOP')
    p.poll()

    sys.stderr.write('%% Waiting for %d deliveries\n' % len(p))
    p.flush()
