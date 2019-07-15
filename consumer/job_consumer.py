import argparse
import sys
from uuid import uuid1
from confluent_kafka import Consumer, KafkaException, Producer
import json
from time import sleep


def print_assignment(_, partitions):
    print('Assignment:', partitions)


def report_job_done(producer: Producer, job_id: int):
    producer.produce('job_report', str(job_id))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", help="Address for the Kafka broker", default="kafka:29092")
    parser.add_argument("--topic", help="Topic to publish messages to", default="job_queue")
    args = parser.parse_args()

    conf = {'bootstrap.servers': args.broker, 'group.id': 'job_consumer', 'session.timeout.ms': 6000,
            'auto.offset.reset': 'earliest', 'max.in.flight.requests.per.connection': 1, 'queued.min.messages': 1}

    consumer = Consumer(conf)
    subscribed = False
    while not subscribed:
        try:
            consumer.subscribe([args.topic], on_assign=print_assignment)
        except:
            continue
        subscribed = True

    stop_consumer = Consumer({'bootstrap.servers': args.broker, 'group.id': uuid1, 'auto.offset.reset': 'earliest'})
    stop_consumer.subscribe(['kill_all_consumers'])

    producer_conf = {'bootstrap.servers': args.broker}
    producer = Producer(**conf)

    while True:
        consumer.subscribe([args.topic])
        stop_msg = stop_consumer.poll(timeout=1.0)
        if stop_msg is not None:
            if stop_msg.value() == b"STOP":
                print("Received STOP message")
                break

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
        consumer.unsubscribe()
        msg_data = json.loads(msg.value())
        print(f'Doing job {msg_data["id"]}')
        sleep(int(msg_data['job_length']))
        report_job_done(producer, msg_data['id'])

    consumer.close()
