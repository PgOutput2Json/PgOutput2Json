-- this must be done in a separate transaction, hence the separate file
\connect test_db

SELECT * FROM pg_create_logical_replication_slot('test_slot', 'pgoutput');
