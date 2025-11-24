FROM python:3.11-slim

WORKDIR /consumer

COPY . .

RUN pip install -r requirements.txt

CMD ["python", "consumer.py"]