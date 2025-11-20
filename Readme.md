{
  "name": "postgres-cdc",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "host.docker.internal",
    "database.port": "5432",
    "database.user": "debezium",
    "database.password": "password", -- pasword for debizium user
    "database.dbname": "postgres",   -- db name
    "database.server.name": "postgres_cdc",
    "topic.prefix": "postgres_cdc",
    "plugin.name": "pgoutput",
    "slot.name": "cdc_slot",
    "table.include.list": "public.products"  -- actucal table which we want to get real time data
  }
}