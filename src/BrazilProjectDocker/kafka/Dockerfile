FROM python:3.9-slim
WORKDIR /app
RUN pip install --no-cache-dir kafka-python pandas numpy
COPY producer.py .
CMD ["python", "producer.py"]
