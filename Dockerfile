FROM python:3.10

COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

RUN mkdir /app
COPY src /app/src
WORKDIR /app
