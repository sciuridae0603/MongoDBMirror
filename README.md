# MongoDB Mirror Tool

Mirrors databases and collections from one MongoDB deployment to another. It
does a full copy first, then keeps the destination up to date by replaying the
source oplog.

The source must be a replica set, since the tool reads `local.oplog.rs`. The
destination can be any MongoDB deployment.

## Requirements

- Python 3.9
- [Pipenv](https://pipenv.pypa.io/)
- Source user: read access to the mirrored databases and to `local.oplog.rs`
- Destination user: write access to the target databases

See [Permissions](#permissions) for the exact roles.

## Permissions

| User | Role | Database | Why |
|------|------|----------|-----|
| Source | `readAnyDatabase` | `admin` | `listDatabases` (needed for `*=*`), `listCollections`, `find`, `listIndexes` |
| Source | `read` | `local` | Read `local.oplog.rs`. `readAnyDatabase` does not cover `local` |
| Destination | `readWriteAnyDatabase` | `admin` | Writes, create/drop collections and indexes, cross-database `renameCollection` |
| Monitor (`type = database`) | `readWrite` | the monitor `database` | Upsert the status document |

To narrow it down, use `read` on each mirrored source database instead of
`readAnyDatabase`, and `readWrite` on each target database instead of
`readWriteAnyDatabase`. A source rename across databases then needs
`readWrite` on both destination databases.

Time series are mirrored through `system.buckets.*`, and the built-in `read`
and `readWrite` roles only document access to non-system collections. Check
your version with `db.getRole("readWrite", {db: "<db>", showPrivileges: true})`.
If the buckets are not covered, add a custom role granting `find` on
`{db: "<db>", collection: "system.buckets.<name>"}` on the source, and `find`,
`insert`, `update`, `remove`, `createCollection`, `createIndex` and
`dropCollection` on the same resource on the destination.

### Ops Manager

When Ops Manager Automation manages authentication, create these users and
custom roles under **Deployment → Security → MongoDB Users / MongoDB Roles**.
Users created with `db.createUser` outside Ops Manager can be removed by
Automation when it enforces a consistent set of users.

## Installation

```bash
pipenv install
```

## Usage

```bash
cp confs/example.conf confs/my.conf   # confs/* is git-ignored, except the example
mkdir -p logs
pipenv run python main.py -c confs/my.conf
```

In `oplog` and `auto` mode the process runs until it is stopped. Run it under a
supervisor such as systemd so it restarts after it exits (see
[Resuming and failures](#resuming-and-failures)).

## Sync modes

| Mode    | Behavior |
|---------|----------|
| `full`  | Copies every mapped collection once, then exits. |
| `oplog` | Replays the oplog from the saved position. With no saved position, or one the oplog no longer covers, it starts from now and logs a warning, so earlier changes are missing. |
| `auto`  | Same as `oplog`, but when the saved position is missing or outdated it runs a full sync first. Oplog pulling starts before the full sync, so writes made during the copy are replayed afterwards. Recommended. |

## Configuration

See [`confs/example.conf`](confs/example.conf). Boolean values must be written
as `true`; anything else counts as false.

### `[sync]`

| Key | Description |
|-----|-------------|
| `mode` | `full`, `oplog` or `auto` (see [Sync modes](#sync-modes)) |
| `source_uri` | Source replica set connection string |
| `destination_uri` | Destination connection string |
| `last_optime_file` | File storing the last applied oplog position |
| `log_file` | Log file path. Logs are also written to stderr. |
| `threads` | Number of collections copied in parallel during full sync |
| `oplog_pull_interval` | Seconds to wait between oplog pulls once caught up |
| `mirror_indexes` | `true` to copy index definitions to the destination |
| `delete_documents_not_in_source` | `true` to delete destination documents that are not in the source during full sync |
| `flag_perfix` | Prefix of the temporary marker field used by `delete_documents_not_in_source` (the key is spelled `perfix`) |

`delete_documents_not_in_source` works by setting a
`<flag_perfix>not_found_in_source` field on every destination document, then
deleting the documents the copy did not overwrite. Pick a prefix that does not
clash with your own fields.

### `[mapping]`

One rule per line, `source=destination`. Names are case sensitive.

```ini
# Mirror every non-system database under the same names
*=*

# All collections in src_db_1 into dest_db_1, including collections created later
src_db_1.*=dest_db_1.*

# A single collection under a new name
src_db_2.data=dest_db_1.data_2
```

When `*=*` is present, every other rule is ignored, and `*` can only map to `*`. The `admin`, `config` and `local` databases are never mirrored.

### `[monitor]` (optional)

Reports the mirror status every second while oplog mirroring runs.

| Key | Description |
|-----|-------------|
| `enabled` | `true` to turn monitoring on |
| `type` | `database` or `script` |
| `mirror_key` | Name identifying this mirror instance |
| `uri`, `database`, `collection` | Where to write the status (`database` type) |
| `command` | Command to run (`script` type) |

The status contains:

| Field | Description |
|-------|-------------|
| `mirror_key` | The configured key |
| `current_time` | Current time in milliseconds |
| `oplog_queue_size` | Oplog entries pulled but not yet applied |
| `last_processed_oplog_timestamp` | Oplog time (milliseconds) of the last applied and saved position, empty before the first one |

With `database`, one document per `mirror_key` is upserted into the collection.
With `script`, the command is run with the four values as arguments in that
order, and its output is logged; see
[`monitor.example.sh`](monitor.example.sh). The replication lag is
`current_time - last_processed_oplog_timestamp`.

## What is mirrored

- Documents, including transactions and batched writes
- Collections and databases created, dropped or renamed while running
  (new collections follow the `*=*` and `db.*` rules)
- Indexes, views, capped, clustered and time series collections
- Time series are mirrored at bucket level, so the source user needs read access
  and the destination user needs write access to `system.buckets.*`

## Resuming and failures

- The last applied oplog position is saved to `last_optime_file` only after the
  changes before it reach the destination, so a restart resumes without losing
  queued changes.
- If the source oplog rolls over past the position being pulled, changes are
  lost and the process exits. Restart it in `auto` mode to run a full sync.
  Size the source oplog to cover the longest expected downtime.
- While replaying the oplog, connection and authorization errors are retried every 5 seconds. Other errors
  on a single operation are logged and that operation is skipped, so check the
  log for `Skipping` and `Error` lines.
- To force a full resync, stop the process, delete `last_optime_file` and start
  it in `auto` mode.

## License

See [LICENSE](LICENSE).
