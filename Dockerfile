FROM confluentinc/cp-kafka:7.6.0 AS kafka

FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-21-jre-headless && \
    rm -rf /var/lib/apt/lists/*

COPY --from=kafka /usr/bin/kafka-run-class /usr/bin/kafka-run-class
COPY --from=kafka /usr/bin/kafka-server-start /usr/bin/kafka-server-start
COPY --from=kafka /usr/bin/kafka-topics /usr/bin/kafka-topics
COPY --from=kafka /usr/bin/zookeeper-server-start /usr/bin/zookeeper-server-start
COPY --from=kafka /usr/share/java /usr/share/java
COPY --from=kafka /etc/kafka/log4j.properties /etc/kafka/log4j.properties

RUN useradd -r -M -d /nonexistent kafkarunner && \
    mkdir -p /var/lib/kafka/data /var/lib/zookeeper/data && \
    chown -R kafkarunner /var/lib/kafka /var/lib/zookeeper

ENV KAFKA_HEAP_OPTS="-Xmx256M -Xms256M"

COPY fly/server.properties /etc/kafka/server.properties
COPY fly/zookeeper.properties /etc/kafka/zookeeper.properties

WORKDIR /app

COPY api/requirements.txt /app/api-requirements.txt
COPY consumers/requirements.txt /app/consumers-requirements.txt
COPY producers/requirements.txt /app/producers-requirements.txt
RUN pip install --no-cache-dir \
    supervisor \
    -r /app/api-requirements.txt \
    -r /app/consumers-requirements.txt \
    -r /app/producers-requirements.txt

COPY api/ /app/api/
COPY dashboard/ /app/dashboard/
COPY consumers/anomaly_detector.py /app/
COPY producers/node_agent.py /app/

COPY fly/supervisord.conf /etc/supervisord.conf
COPY fly/start.sh /app/start.sh
RUN chmod +x /app/start.sh

ENV KAFKA_BROKER=localhost:9092
EXPOSE 8000

CMD ["/app/start.sh"]
