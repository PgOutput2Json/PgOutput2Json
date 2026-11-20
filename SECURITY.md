# Security Policy

## Supported Versions

Security fixes go into the latest release: upgrade to it to get a fix. All the packages (`PgOutput2Json` and its
outputs) are released together, with the same version.

## Reporting a Vulnerability

Please don't open a public issue. Report it privately on GitHub, in the repository's
[Security tab](https://github.com/PgOutput2Json/PgOutput2Json/security/advisories/new) ("Report a vulnerability").

Include what's affected (the package and its version), what an attacker can do, and how to reproduce it. You'll get
an answer within a week. Once it's fixed and released, the advisory is published, with credit to you if you want it.

## What's in Scope

- **SQL and queries built by the library and the outputs**: the initial copy, the table sync state, and the tables,
  rows and queries the stores (SQLite, MongoDB, DynamoDB, ClickHouse) write. A way to inject SQL or a query through a
  row's values, or through a table or column name of a published table, is a vulnerability.
- **Messages and requests**: a row's values ending up anywhere other than the message body, the key or the partition
  key (for example in the headers of a webhook request, or in a routing key the output did not build for it).
- **Webhook signatures**: a way to forge or replay a request that passes the signature check as documented.
- **Secrets**: connection strings, passwords, API keys or signing secrets written to logs, messages or the stores.

Not in scope:

- **The replication user reads the published tables.** Whoever has its connection string can read every change of
  them, as with PostgreSQL's own logical replication. Give it only the access described in the docs.
- **Consumers trust the messages.** Whoever can write to the output (the broker, the store, the webhook endpoint
  without signature checks) can send messages that look like changes. Secure the outputs as you would any other
  service.
- The security of PostgreSQL, the brokers and the stores themselves.
