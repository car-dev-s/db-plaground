FROM confluentinc/cp-kafka-connect:7.7.1

# Apache Iceberg Kafka Connect sink. Not published to Maven Central - distributed
# as a runtime .zip (jar + all deps) via GitHub releases of the connector's repo
# (originally tabular-io/iceberg-kafka-connect, now databricks/iceberg-kafka-connect).
# Must land in its own subdirectory under a plugin.path entry - Connect's plugin
# scanner treats each subdirectory as one isolated plugin, but ignores loose jars
# dropped directly in the plugin.path directory itself.
ARG ICEBERG_CONNECT_VERSION=0.6.19
RUN curl -sSL -o /tmp/iceberg-kafka-connect-runtime.zip \
    "https://github.com/databricks/iceberg-kafka-connect/releases/download/v${ICEBERG_CONNECT_VERSION}/iceberg-kafka-connect-runtime-${ICEBERG_CONNECT_VERSION}.zip" && \
    python3 -m zipfile -e /tmp/iceberg-kafka-connect-runtime.zip /usr/share/java/ && \
    rm /tmp/iceberg-kafka-connect-runtime.zip
