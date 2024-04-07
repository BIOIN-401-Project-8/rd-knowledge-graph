FROM nvcr.io/nvidia/pytorch:23.11-py3

COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

RUN mkdir /app
COPY src /app/src
WORKDIR /app
