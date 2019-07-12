import argparse
import sys
from confluent_kafka import Producer


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", help="Address for the Kafka broker", default="localhost")
    parser.add_argument("--topic", help="Topic to publish messages to", default="job_queue")
    parser.add_argument("--number-of-jobs", default=100)
    args = parser.parse_args()
    print(args.broker)

    conf = {'bootstrap.servers': args.broker}
    p = Producer(**conf)

    job_length = 5
    for message_id in range(args.number_of_jobs):
        p.produce(args.topic, f'{{"id": "{message_id}", "job_length": "{job_length}"}}')
        p.poll()

    sys.stderr.write('%% Waiting for %d deliveries\n' % len(p))
    p.flush()
