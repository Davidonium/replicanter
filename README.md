# Replicanter

The goal of this project is to aid in projects that require data migration from a mysql to another datasource.
It can be another mysql, postgresql, redis, elasticsearch, websockets, etc.

The dependency library `go-mysql` exposes rows in a very "raw" way. For example, enums are numbers, not the string value.
Update statement rows are in pairs of before and after and are not that easy to identify. 
Also the `canal` package uses mysqldump to grab previous state, this project tries to avoid that.
This project tries to simplify that and expose a simple interface to read row events and move them to other datasources.

Also my goal is to provide some executables and docker containers 
that through configuration, write the data to sources like rabbitmq, amazon sqs, or websockets.