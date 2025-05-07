FROM python:3-slim
WORKDIR /programas/ingesta

RUN apt-get update && \
    apt-get install -y libpq-dev && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install boto3 psycopg2  
COPY . .
CMD [ "python3", "./ingesta2.py" ]
