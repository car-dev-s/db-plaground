CREATE KEYSPACE IF NOT EXISTS playground
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS playground.https_sessions (
    source_ip         text,
    timestamp          timestamp,
    source_port        int,
    destination_ip     text,
    destination_port   int,
    domain             text,
    method             text,
    status_code        int,
    bytes_sent         bigint,
    bytes_received     bigint,
    duration_millis    bigint,
    timestamp_iso      text,
    PRIMARY KEY ((source_ip), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);
