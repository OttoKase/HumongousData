$body = @{
    name = "cdc-connector"
    config = @{
        "connector.class" = "io.debezium.connector.postgresql.PostgresConnector"
        "database.hostname" = "postgres"
        "database.port" = "5432"
        "database.user" = "cdc_user"
        "database.password" = "admin"
        "database.dbname" = "sourcedb"
        "topic.prefix" = "dbserver1"
        "table.include.list" = "public.customers,public.drivers"
        "plugin.name" = "pgoutput"
        "slot.name" = "debezium_slot"
        "publication.name" = "dbz_publication"
    }
} | ConvertTo-Json -Depth 5

Invoke-RestMethod `
    -Uri "http://localhost:8083/connectors" `
    -Method Post `
    -ContentType "application/json" `
    -Body $body