FROM python:3.11-slim

WORKDIR /consumer

COPY . .

RUN pip install -r requirements.txt

ENTRYPOINT ["python", "main.py"]

CMD ["default_message", "10"]