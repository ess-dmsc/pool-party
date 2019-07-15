# Prototype job distribution for a pool of workers via Kafka

Requires docker-compose to run.

Run with:
```
docker-compose up -d --scale consumer=10 --build
```

and bring down with
```
docker-compose down -v
```

## What it does

The producer publishes 100 jobs to a `job_queue` topic. Each job consists of a JSON with an `id` field and a random `job_length` of between 1 and 9 seconds.

10 consumers are started and consume jobs from the `job_queue`. As soon as they recieve a job they unsubscribe from the topic so that they do not recieve another job until they have finished the one they already have. Carrying out a job consists of sleeping for the length of time specifed in `job_length` and then reporting that the job is finished by publishing job `id` to a `job_report` topic. They then resubscribe to receive their next job.

Once the producer has published all the jobs it collects messages from the `job_report` topic until it has received reports for every job, prints a sorted list of the job IDs to screen and then tells the consumers to stop.

If each job was completed once then the output list from the producer will be consecutive numbers 0 to 99.
