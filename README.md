# MDB Index Sync
Sync indexes from one MDB instance to another.

Allows deleting all indexes at the destination DB instance to ensure
replication. Currently we only support copying over the name, but optional
index args should be easy to add.

## Why Do We Need This?
Often I've found a normal workflow is:

1. Have a staging DB that you test stuff out in.
2. Have a prod DB you run prod against.
3. Fine tune indexes in prod due to data/usage/pressure from stakeholders.
4. Indexes become out of sync.

Index synx allows you to keep two DBs in sync, so the out-of-sync issue is
less of an issue. This can probably be run in CI/CD or a cronjob or something
if you're so inclined. Index removal probably only needs to be run sparringly,
or when you first decide to keep your DBs in sync.
