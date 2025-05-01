FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y redis-tools && apt-get clean

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py *.sh ./
RUN chmod +x *.sh

RUN chmod -R 777 /app

# Default command (can be overridden in docker-compose.yml)
CMD ["python", "coordinator.py"]