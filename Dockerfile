FROM maven:3.9.8-eclipse-temurin-21 AS builder

WORKDIR /build
COPY . .
RUN mvn clean install -DskipTests -Passembly

FROM confluentinc/cp-kafka-connect:7.9.0

ENV CONNECT_LOG4J_LOGGERS=org.reflections=ERROR

COPY --from=builder /build/target/debezium-connector-jdbc-*-plugin.tar.gz /tmp/
RUN mkdir -p /usr/share/confluent-hub-components/debezium-connector-jdbc && \
    tar -xzf /tmp/debezium-connector-jdbc-*-plugin.tar.gz -C /usr/share/confluent-hub-components/
RUN echo "✅ Installed Debezium connector JARs:" && find /usr/share/confluent-hub-components/debezium-connector-jdbc -name '*.jar'
USER appuser


#FROM confluentinc/cp-kafka-connect:7.9.0
#
##ENV CONNECT_LOG4J_LOGGERS=io.debezium=DEBUG,org.hibernate.SQL=DEBUG
##ENV LOG4J_LOGGERS=io.debezium=DEBUG,org.hibernate.SQL=DEBUG
#
#COPY ./target/debezium-connector-jdbc-*-plugin.tar.gz /tmp/
#
#RUN mkdir -p /usr/share/confluent-hub-components/debezium-connector-jdbc && \
#    tar -xzf /tmp/debezium-connector-jdbc-*-plugin.tar.gz -C /usr/share/confluent-hub-components/debezium-connector-jdbc --strip-components=1
#
## Optional: Show what was installed
#RUN echo "✅ Installed Debezium connector JARs:" && find /usr/share/confluent-hub-components/debezium-connector-jdbc -name '*.jar'
#
#USER appuser