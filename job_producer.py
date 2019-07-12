import argparse
import sys
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic


def create_job_queue_topic(topic_name: str, number_of_partitions: int, broker: str):
    admin_client = AdminClient({'bootstrap.servers': broker})
    fs = admin_client.create_topics([NewTopic(topic_name, num_partitions=3, replication_factor=number_of_partitions)])

    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print("Topic {} created".format(topic))
        except Exception as e:
            print("Failed to create topic {}: {}".format(topic, e))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", help="Address for the Kafka broker", default="localhost")
    parser.add_argument("--topic", help="Topic to publish messages to", default="job_queue")
    parser.add_argument("--number-of-jobs", default=100)
    parser.add_argument("--consumers-in-pool", default=10)
    args = parser.parse_args()

    create_job_queue_topic(args.topic, args.consumers_in_pool, args.broker)

    conf = {'bootstrap.servers': args.broker}
    p = Producer(**conf)

    job_length = 5
    for message_id in range(args.number_of_jobs):
        p.produce(args.topic, f'{{"id": "{message_id}", "job_length": "{job_length}"}}')
        p.poll()

    sys.stderr.write('%% Waiting for %d deliveries\n' % len(p))
    p.flush()
