FROM python:3

ADD job_producer.py /
ADD ../requirements.txt /

RUN pip install -r requirements.txt

CMD [ "python", "./job_consumer.py" ]
