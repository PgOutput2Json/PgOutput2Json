# PgOutput2Json
PgOutput2Json library uses PostgreSQL logical replication to push changes, as JSON messages, from database tables to a message broker, such as RabbitMq (implemented) or Redis (todo). 

⚠️ **PgOutput2Json is in early stages of development. Use at your own risk.**  
⚠️ **Npgsql is used for connecting to PostgreSQL. Replication support is new in Npgsql and is considered a bit experimental.** 
