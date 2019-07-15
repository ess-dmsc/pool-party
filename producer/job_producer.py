import argparse
import sys
from time import sleep
from confluent_kafka import Producer, Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from uuid import uuid1
import random


def create_job_queue_topic(topic_name: str, number_of_partitions: int, broker: str, stop_topic: str, job_report_topic: str):
    print("Checking if Kafka is up...", flush=True)
    admin_client = AdminClient({'bootstrap.servers': broker})

    kafka_up = False
    while not kafka_up:
        try:
            print("Checking if Kafka is up...")
            admin_client.list_topics(timeout=10)
        except:
            continue
        kafka_up = True

    print("Kafka is up!")

    fs = admin_client.create_topics([NewTopic(topic_name, num_partitions=number_of_partitions, replication_factor=1),
                                     NewTopic(stop_topic, num_partitions=1, replication_factor=1),
                                     NewTopic(job_report_topic, num_partitions=1, replication_factor=1)])

    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print("Topic {} created".format(topic))
        except Exception as e:
            print("Failed to create topic {}: {}".format(topic, e))


def check_job_reports(number_of_jobs: int):
    conf = {'bootstrap.servers': args.broker, 'group.id': uuid1(), 'session.timeout.ms': 6000,
            'auto.offset.reset': 'earliest', 'max.in.flight.requests.per.connection': 1}

    consumer = Consumer(conf)
    consumer.subscribe(['job_report'])

    jobs_done = []
    while len(jobs_done) < number_of_jobs:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        else:
            # Proper message
            sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                             (msg.topic(), msg.partition(), msg.offset(),
                              str(msg.key())))
        jobs_done.append(int(msg.value()))
    print(jobs_done.sort())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", help="Address for the Kafka broker", default="kafka:29092")
    parser.add_argument("--topic", help="Topic to publish messages to", default="job_queue")
    parser.add_argument("--number-of-jobs", default=100)
    parser.add_argument("--consumers-in-pool", default=10)
    args = parser.parse_args()

    stop_topic = 'kill_all_consumers'
    job_report_topic = 'job_report'
    create_job_queue_topic(args.topic, args.consumers_in_pool, args.broker, stop_topic, job_report_topic)

    conf = {'bootstrap.servers': args.broker}
    p = Producer(**conf)

    print("Start producing job messages")
    sleep(20)  # Wait plenty of time for topic to initialise
    for message_id in range(args.number_of_jobs):
        p.produce(args.topic, f'{{"id": "{message_id}", "job_length": "{random.randint(1,9)}"}}')
        p.poll(timeout=5)

    check_job_reports(args.number_of_jobs)

    p.produce(stop_topic, 'STOP')
    p.poll(timeout=5)

    sys.stderr.write('%% Waiting for %d deliveries\n' % len(p))
    p.flush()
