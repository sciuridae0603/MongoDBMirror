import argparse
import configparser
import json
import os
import queue
import subprocess
import sys
import threading
import time

import bson
import pymongo
from bson import Timestamp
from jsondiff import diff as jsondiff
from loguru import logger
from pymongo import DeleteOne, ReplaceOne
from pymongo.errors import (
    BulkWriteError,
    CollectionInvalid,
    ConnectionFailure,
    OperationFailure,
)


class Flags:
    def __init__(self):
        self.not_found_in_source = None


class GlobalVariables:
    def __init__(self):
        self.args = None
        self.config = None
        self.flags = Flags()
        self.mapping = {}  # source ns -> destination ns
        self.explicit_mapping = set()  # source ns written literally in [mapping]
        self.mirror_all = False
        self.db_wildcards = {}  # source db -> destination db for "src.*=dst.*"
        self.missed_updates = (
            {}
        )  # source ns -> {id key: _id} updates not found in source
        self.source_db = None
        self.destination_db = None
        self.log_collection = None

        self.full_sync_queue = queue.Queue()
        self.oplog_sync_queue = queue.Queue(maxsize=1024)
        self.last_pulled_oplog_timestamp = None  # puller position
        self.last_applied_oplog_timestamp = None  # saved position, all applied


g = GlobalVariables()

SYSTEM_DATABASES = ["admin", "config", "local"]
BUCKETS_PREFIX = "system.buckets."
COLLECTION_COMMANDS = (
    "create",
    "drop",
    "createIndexes",
    "commitIndexBuild",
    "dropIndexes",
)
BATCH_SIZE = 1000  # documents per bulk write, oplogs per apply batch
OPLOG_FETCH_LIMIT = 10000  # oplogs read per pull, bounds memory while catching up
MAX_WINDOW_SECONDS = 3600
RETRY_SECONDS = 5
UNAUTHORIZED = 13
NAMESPACE_NOT_FOUND = 26
INDEX_NOT_FOUND = 27


def parse_args():
    parser = argparse.ArgumentParser(description="MongoDB mirror tool")
    parser.add_argument(
        "-c",
        "--config",
        type=str,
        required=True,
        help="Path to the configuration file",
    )
    return parser.parse_args()


def get_database_and_collection_from_mapping(mapping):
    database = mapping.split(".")[0]
    collection = mapping.split(".")[1:]
    collection = ".".join(collection)
    return database, collection


def format_timestamp(timestamp):
    return f"{timestamp.time}.{timestamp.inc}" if timestamp else None


def read_config(path):
    logger.info(f"Reading configuration from {path}")
    try:
        g.config = configparser.RawConfigParser()
        g.config.optionxform = str  # keep mapping keys case sensitive
        g.config.read(path)

        g.flags.not_found_in_source = (
            g.config["sync"]["flag_perfix"] + "not_found_in_source"
        )

        logger.add(g.config["sync"]["log_file"])
    except Exception as e:
        logger.error(f"Error reading configuration file: {e}")
        logger.error(e)
        sys.exit(1)


def connect_to_mongodb():
    logger.info("Connecting to source MongoDB...")
    try:
        g.source_db = pymongo.MongoClient(g.config["sync"]["source_uri"])
        logger.info("Connected to source MongoDB")
    except Exception as e:
        logger.error(f"Error connecting to source MongoDB: {e}")
        logger.error(e)
        sys.exit(1)

    logger.info("Connecting to destination MongoDB...")
    try:
        g.destination_db = pymongo.MongoClient(g.config["sync"]["destination_uri"])
        logger.info("Connected to destination MongoDB")
    except Exception as e:
        logger.error(f"Error connecting to destination MongoDB: {e}")
        logger.error(e)
        sys.exit(1)

    if g.config.has_section("monitor") and g.config["monitor"]["enabled"] == "true":
        if g.config["monitor"]["type"] == "database":
            logger.info("Connecting to log database...")
            try:
                g.log_collection = pymongo.MongoClient(g.config["monitor"]["uri"])[
                    g.config["monitor"]["database"]
                ][g.config["monitor"]["collection"]]
                logger.info("Connected to log database")
            except Exception as e:
                logger.error(f"Error connecting to log database: {e}")
                logger.error(e)
                sys.exit(1)


def init_mapping():
    logger.info("Initializing mirror mapping...")
    rules = g.config["mapping"]
    if "*" in rules:
        if rules["*"] != "*":
            logger.error("Invalid mapping configuration for *")
            sys.exit(1)
        g.mirror_all = True
        databases = [
            database
            for database in g.source_db.list_database_names()
            if database not in SYSTEM_DATABASES
        ]
    else:
        databases = set()
        for key in rules:
            if len(key.split(".")) < 2 or len(rules[key].split(".")) < 2:
                logger.error(f"Invalid mapping configuration for {key}")
                sys.exit(1)
            database, collection = get_database_and_collection_from_mapping(key)
            if collection == "*":
                g.db_wildcards[database] = rules[key].split(".")[0]
            else:
                g.mapping[key] = rules[key]
                g.explicit_mapping.add(key)
            databases.add(database)

    for database in databases:
        for collection in g.source_db[database].list_collection_names():
            resolve_mapping(database + "." + collection)


def resolve_mapping(source_ns):
    """Destination ns for a source ns, learning collections created after startup."""
    if source_ns in g.mapping:
        return g.mapping[source_ns]
    database, collection = get_database_and_collection_from_mapping(source_ns)
    if collection.startswith(BUCKETS_PREFIX):
        # time series data lives in its buckets collection, mapped like the time series itself
        target = resolve_mapping(database + "." + collection[len(BUCKETS_PREFIX) :])
        if target is None:
            return None
        target_database, target_collection = get_database_and_collection_from_mapping(
            target
        )
        target = target_database + "." + BUCKETS_PREFIX + target_collection
    elif not collection or collection.startswith("system."):
        return None
    elif g.mirror_all and database not in SYSTEM_DATABASES:
        target = source_ns
    elif database in g.db_wildcards:
        target = g.db_wildcards[database] + "." + collection
    else:
        return None
    g.mapping[source_ns] = target
    logger.info(f"Mapped {source_ns} to {target}")
    return target


def forget_mapping(source_ns):
    # keep explicit rules so a recreated collection is mirrored again
    if source_ns not in g.explicit_mapping:
        g.mapping.pop(source_ns, None)


def source_collection_info(source_ns):
    database, collection = get_database_and_collection_from_mapping(source_ns)
    for info in g.source_db[database].list_collections(filter={"name": collection}):
        return info
    return None


def create_destination_collection(database, name, options):
    options = {
        key: value
        for key, value in options.items()
        if key not in ("create", "idIndex", "temp")
    }
    try:
        database.create_collection(name, **options)
        logger.info(f"Created {database.name}.{name}")
    except CollectionInvalid:
        pass  # already exists


def mirror_view(source_ns, options):
    if options["viewOn"].startswith(BUCKETS_PREFIX):
        return  # view of a time series, created together with its buckets
    destination_ns = resolve_mapping(source_ns)
    if destination_ns is None:
        return
    destination_database, destination_collection = (
        get_database_and_collection_from_mapping(destination_ns)
    )
    database, _ = get_database_and_collection_from_mapping(source_ns)
    options = dict(options)
    view_on = resolve_mapping(database + "." + options["viewOn"])
    if view_on and view_on.startswith(destination_database + "."):
        options["viewOn"] = get_database_and_collection_from_mapping(view_on)[1]

    logger.info(f"Mirroring view {source_ns} to {destination_ns}")
    g.destination_db[destination_database].drop_collection(destination_collection)
    g.destination_db[destination_database].create_collection(
        destination_collection, **options
    )


def prepare_destination(source_ns):
    """Create the destination collection, view or time series like the source; returns the source type."""
    info = source_collection_info(source_ns)
    if info is None:
        return None
    destination_database, destination_collection = (
        get_database_and_collection_from_mapping(g.mapping[source_ns])
    )
    database = g.destination_db[destination_database]
    if info["type"] == "view":
        mirror_view(source_ns, info["options"])
    elif not destination_collection.startswith(BUCKETS_PREFIX):
        if not database.list_collection_names(filter={"name": destination_collection}):
            create_destination_collection(
                database, destination_collection, info["options"]
            )
    return info["type"]


def index_spec(spec):
    return {key: value for key, value in spec.items() if key not in ("v", "ns")}


def create_index_from_spec(collection, spec):
    keys = spec["key"]
    keys = list(keys.items()) if isinstance(keys, dict) else keys
    options = {
        key: value
        for key, value in index_spec(spec).items()
        if key not in ("key", "createIndexes")
    }
    collection.create_index(keys, **options)


def mirror_indexes(source_ns):
    source_database, source_collection = get_database_and_collection_from_mapping(
        source_ns
    )
    destination_database, destination_collection = (
        get_database_and_collection_from_mapping(g.mapping[source_ns])
    )
    destination = g.destination_db[destination_database][destination_collection]
    logger.info(f"Mirroring indexes from {source_ns} to {g.mapping[source_ns]}")
    source_indexes = g.source_db[source_database][source_collection].index_information()
    destination_indexes = destination.index_information()

    for index in source_indexes:
        if index == "_id_":
            continue
        spec = dict(source_indexes[index], name=index)
        if index not in destination_indexes:
            logger.info(f"Creating index {index} in {g.mapping[source_ns]}")
            try:
                create_index_from_spec(destination, spec)
            except OperationFailure as e:
                logger.error(
                    f"Error creating index {index} in {g.mapping[source_ns]}: {e}"
                )
        elif jsondiff(
            index_spec(source_indexes[index]), index_spec(destination_indexes[index])
        ):
            logger.info(f"Updating index {index} in {g.mapping[source_ns]}")
            destination.drop_index(index)
            while index in destination.index_information():
                time.sleep(3)
            create_index_from_spec(destination, spec)

    # Append index that mirror tool needs (used when check destination document not exists in source)
    if (
        not source_collection.startswith(BUCKETS_PREFIX)
        and g.flags.not_found_in_source not in destination_indexes
    ):
        logger.info(
            f"Creating index {g.flags.not_found_in_source} in {g.mapping[source_ns]}"
        )
        destination.create_index(
            [(g.flags.not_found_in_source, pymongo.ASCENDING)],
            name=g.flags.not_found_in_source,
        )


def replace_documents(collection, documents):
    """Upsert documents in bulk; returns the documents that failed."""
    try:
        collection.bulk_write(
            [ReplaceOne({"_id": d["_id"]}, d, upsert=True) for d in documents],
            ordered=False,
        )
        return []
    except BulkWriteError as e:
        errors = e.details["writeErrors"]
        for error in errors:
            logger.error(
                f"Error syncing document {documents[error['index']]['_id']} to {collection.full_name}: {error['errmsg']}"
            )
        return [documents[error["index"]] for error in errors]


def sync_collection(source_ns):
    source_database, source_collection = get_database_and_collection_from_mapping(
        source_ns
    )
    destination_ns = g.mapping[source_ns]
    destination_database, destination_collection = (
        get_database_and_collection_from_mapping(destination_ns)
    )
    source = g.source_db[source_database][source_collection]
    destination = g.destination_db[destination_database][destination_collection]
    delete_missing = g.config["sync"]["delete_documents_not_in_source"] == "true"
    # buckets have a validator that rejects the flag field, so track their _ids instead
    is_buckets = source_collection.startswith(BUCKETS_PREFIX)
    flag = g.flags.not_found_in_source

    total = source.estimated_document_count()
    logger.info(f"Syncing {source_ns} to {destination_ns}")

    if delete_missing and not is_buckets:
        destination.update_many({}, {"$set": {flag: True}})

    count = 0
    failed_documents = []
    source_ids = set()
    batch = []
    for document in source.find():
        batch.append(document)
        if is_buckets:
            source_ids.add(document["_id"])
        if len(batch) == BATCH_SIZE:
            failed_documents += replace_documents(destination, batch)
            count += len(batch)
            batch = []
            logger.info(
                f"Syncing {source_ns} to {destination_ns} ({count}/{total} documents)"
            )
    if batch:
        failed_documents += replace_documents(destination, batch)
        count += len(batch)

    if failed_documents:
        logger.info(f"Retrying {len(failed_documents)} failed documents of {source_ns}")
        for document in replace_documents(destination, failed_documents):
            logger.error(f"Still error syncing document {document['_id']}")

    if delete_missing:
        if is_buckets:
            stale = [
                d["_id"]
                for d in destination.find({}, {"_id": 1})
                if d["_id"] not in source_ids
            ]
            destination.delete_many({"_id": {"$in": stale}})
        else:
            destination.delete_many({flag: True})

    logger.info(f"Syncing {source_ns} to {destination_ns} complete ({count} documents)")


def copy_from_source(source_ns):
    if prepare_destination(source_ns) == "collection":
        sync_collection(source_ns)


def sync_worker():
    while True:
        try:
            source_ns = g.full_sync_queue.get_nowait()
        except queue.Empty:
            return
        try:
            sync_collection(source_ns)
        except Exception as e:
            logger.error(f"Error syncing {source_ns}, put back to queue: {e}")
            time.sleep(RETRY_SECONDS)
            g.full_sync_queue.put(source_ns)
        finally:
            g.full_sync_queue.task_done()


def full_sync(namespaces):
    logger.info("Starting full sync")
    for source_ns in namespaces:
        g.full_sync_queue.put(source_ns)

    for _ in range(int(g.config["sync"]["threads"])):
        threading.Thread(target=sync_worker, daemon=True).start()

    g.full_sync_queue.join()
    logger.info("Full sync complete")


def read_last_oplog():
    try:
        with open(g.config["sync"]["last_optime_file"], "r", encoding="utf-8") as f:
            data = json.load(f)
            return Timestamp(data["t"], data["i"])
    except FileNotFoundError:
        return None
    except Exception as e:
        logger.error(f"Error reading last optime: {e}")
        return None


def save_last_oplog(timestamp):
    path = g.config["sync"]["last_optime_file"]
    try:
        # write then rename, so a crash never leaves a half-written file
        with open(path + ".tmp", "w", encoding="utf-8") as f:
            json.dump({"t": timestamp.time, "i": timestamp.inc}, f)
        os.replace(path + ".tmp", path)
    except Exception as e:
        logger.error(f"Error saving last optime: {e}")


def edge_oplog_timestamp(direction):
    """Oldest (1) or newest (-1) oplog timestamp on the source."""
    oplogs = g.source_db["local"]["oplog.rs"].find({}, {"ts": 1})
    for oplog in oplogs.sort("$natural", direction).limit(1):
        return oplog["ts"]
    return None


def start_position():
    """Where to start pulling oplogs, and whether the saved position is unusable."""
    saved = read_last_oplog()
    oldest = edge_oplog_timestamp(1)
    if oldest is None:
        logger.error("Source has no oplog, it must be a replica set")
        sys.exit(1)
    if saved is not None and oldest <= saved:
        logger.info(f"Resuming oplog from {format_timestamp(saved)}")
        return saved, False
    if saved is None:
        logger.info("No saved oplog position")
    else:
        logger.warning(
            f"Saved oplog position {format_timestamp(saved)} is older than the oplog ({format_timestamp(oldest)})"
        )
    return edge_oplog_timestamp(-1), True


def flatten_oplog(oplog):
    """Expand applyOps (transactions, batched inserts) into single oplogs."""
    if oplog["op"] == "c" and "applyOps" in oplog["o"]:
        for inner in oplog["o"]["applyOps"]:
            yield from flatten_oplog({**inner, "ts": oplog["ts"]})
    else:
        yield oplog


def view_id(oplog):
    return (oplog.get("o2") or oplog["o"])["_id"]


def oplog_in_scope(oplog):
    database, collection = get_database_and_collection_from_mapping(oplog["ns"])
    if oplog["op"] == "c":
        command = oplog["o"]
        if "dropDatabase" in command:
            return any(ns.split(".")[0] == database for ns in list(g.mapping))
        if "renameCollection" in command:
            return (
                resolve_mapping(command["renameCollection"]) is not None
                or resolve_mapping(command["to"]) is not None
            )
        for key in COLLECTION_COMMANDS:
            if key in command:
                return resolve_mapping(database + "." + command[key]) is not None
        return False
    if collection == "system.views":
        return resolve_mapping(view_id(oplog)) is not None
    return resolve_mapping(oplog["ns"]) is not None


def pull_oplogs():
    """Queue in-scope oplogs after the pulled position; returns True when caught up."""
    start = g.last_pulled_oplog_timestamp
    oldest = edge_oplog_timestamp(1)
    if oldest > start:
        logger.error(
            f"Oplog rolled over past {format_timestamp(start)}, changes were lost. Exiting so auto mode can full sync on restart"
        )
        os._exit(1)

    latest = edge_oplog_timestamp(-1)
    end = min(latest, Timestamp(start.time + MAX_WINDOW_SECONDS, 0))
    oplogs = list(
        g.source_db["local"]["oplog.rs"]
        .find(
            {
                "op": {"$in": ["i", "u", "d", "c"]},
                "ts": {"$gte": start, "$lte": end},
            }
        )
        .sort("$natural", 1)
        .limit(OPLOG_FETCH_LIMIT)
    )
    if len(oplogs) == OPLOG_FETCH_LIMIT:
        end = oplogs[-1]["ts"]

    count = 0
    for oplog in oplogs:
        if oplog["ts"] <= start:
            continue
        for op in flatten_oplog(oplog):
            if oplog_in_scope(op):
                g.oplog_sync_queue.put(op)
                count += 1

    if end != start:
        g.oplog_sync_queue.put({"op": "checkpoint", "ts": end})
        g.last_pulled_oplog_timestamp = end
    if count:
        logger.info(f"Got {count} oplogs up to {format_timestamp(end)}")
    return end == latest


def oplog_puller():
    def oplog_puller_worker():
        while True:
            try:
                caught_up = pull_oplogs()
            except Exception as e:
                logger.error(f"Error pulling oplogs: {e}")
                caught_up = True
            if caught_up:
                time.sleep(int(g.config["sync"]["oplog_pull_interval"]))

    threading.Thread(target=oplog_puller_worker, daemon=True).start()


def oplog_monitor():
    while True:
        if g.config.has_section("monitor") and g.config["monitor"]["enabled"] == "true":

            mirror_key = g.config["monitor"]["mirror_key"]
            current_time = time.time() * 1000
            oplog_queue_size = g.oplog_sync_queue.qsize()
            # milliseconds, comparable with current_time
            last_processed_oplog_timestamp = (
                g.last_applied_oplog_timestamp.time * 1000
                if g.last_applied_oplog_timestamp
                else None
            )

            if g.config["monitor"]["type"] == "database":
                if g.log_collection is not None:
                    g.log_collection.update_one(
                        {"mirror_key": mirror_key},
                        {
                            "$set": {
                                "mirror_key": mirror_key,
                                "current_time": current_time,
                                "oplog_queue_size": oplog_queue_size,
                                "last_processed_oplog_timestamp": last_processed_oplog_timestamp,
                            }
                        },
                        upsert=True,
                    )
            elif g.config["monitor"]["type"] == "script":
                command = g.config["monitor"]["command"]
                if command:
                    try:
                        args = command.split(" ") + [
                            str(mirror_key),
                            str(int(current_time)),
                            str(oplog_queue_size),
                            (
                                str(last_processed_oplog_timestamp)
                                if last_processed_oplog_timestamp is not None
                                else ""
                            ),
                        ]
                        process = subprocess.Popen(
                            args, stdout=subprocess.PIPE, stderr=subprocess.PIPE
                        )
                        stdout, stderr = process.communicate()
                        if stdout:
                            logger.info(
                                f"Monitor script output: {stdout.decode('utf-8')}"
                            )
                        if stderr:
                            logger.error(
                                f"Monitor script error: {stderr.decode('utf-8')}"
                            )
                        process.wait()
                    except FileNotFoundError:
                        logger.error(f"Monitor script not found at {command}")
                    except Exception as e:
                        logger.error(f"Error executing monitor script: {e}")

        time.sleep(1)


def with_retry(function, *args):
    """Retry while the servers are unreachable; log and skip other errors so one bad oplog cannot stop the mirror."""
    while True:
        try:
            return function(*args)
        except ConnectionFailure as e:
            logger.error(f"Connection error, retrying in {RETRY_SECONDS}s: {e}")
        except OperationFailure as e:
            if e.code != UNAUTHORIZED:
                logger.error(f"Skipping {function.__name__}: {e}")
                return
            logger.error(f"Unauthorized, retrying in {RETRY_SECONDS}s: {e}")
        except Exception as e:
            logger.exception(f"Skipping {function.__name__}: {e}")
            return
        time.sleep(RETRY_SECONDS)


def is_document_oplog(oplog):
    return oplog["op"] in ("i", "u", "d") and not oplog["ns"].endswith(".system.views")


def id_key(value):
    return bson.encode({"_id": value})


def write_in_order(collection, requests):
    """Ordered bulk write that skips a failing write instead of dropping the rest."""
    while requests:
        try:
            collection.bulk_write(requests, ordered=True)
            return
        except BulkWriteError as e:
            error = e.details["writeErrors"][0]
            logger.error(
                f"Skipping oplog write on {collection.full_name}: {error['errmsg']}"
            )
            requests = requests[error["index"] + 1 :]


def apply_document_oplogs(oplogs):
    """Apply consecutive i/u/d oplogs of one namespace with one source read and one bulk write."""
    source_ns = oplogs[0]["ns"]
    destination_ns = resolve_mapping(source_ns)
    if destination_ns is None:
        return
    source_database, source_collection = get_database_and_collection_from_mapping(
        source_ns
    )
    destination_database, destination_collection = (
        get_database_and_collection_from_mapping(destination_ns)
    )

    # updates are hard to parse, so replace with the current source document
    updated_ids = [oplog["o2"]["_id"] for oplog in oplogs if oplog["op"] == "u"]
    current = {}
    if updated_ids:
        for document in g.source_db[source_database][source_collection].find(
            {"_id": {"$in": updated_ids}}
        ):
            current[id_key(document["_id"])] = document

    requests = []
    for oplog in oplogs:
        if oplog["op"] == "i":
            g.missed_updates.get(source_ns, {}).pop(id_key(oplog["o"]["_id"]), None)
            requests.append(
                ReplaceOne({"_id": oplog["o"]["_id"]}, oplog["o"], upsert=True)
            )
        elif oplog["op"] == "u":
            key = id_key(oplog["o2"]["_id"])
            document = current.get(key)
            if document:
                requests.append(
                    ReplaceOne({"_id": document["_id"]}, document, upsert=True)
                )
            else:
                # deleted (a later delete handles it) or the collection was renamed since
                g.missed_updates.setdefault(source_ns, {})[key] = oplog["o2"]["_id"]
        elif oplog["op"] == "d":
            g.missed_updates.get(source_ns, {}).pop(id_key(oplog["o"]["_id"]), None)
            requests.append(DeleteOne({"_id": oplog["o"]["_id"]}))

    write_in_order(
        g.destination_db[destination_database][destination_collection], requests
    )


def drop_destination(source_ns):
    destination_ns = resolve_mapping(source_ns)
    if destination_ns is None:
        return
    destination_database, destination_collection = (
        get_database_and_collection_from_mapping(destination_ns)
    )
    if destination_collection.startswith(BUCKETS_PREFIX):
        # dropping the time series drops its buckets
        destination_collection = destination_collection[len(BUCKETS_PREFIX) :]
    g.missed_updates.pop(source_ns, None)
    logger.info(f"Dropping {destination_ns} (source {source_ns} dropped)")
    g.destination_db[destination_database].drop_collection(destination_collection)
    forget_mapping(source_ns)


def rename_destination(from_ns, to_ns):
    from_destination = resolve_mapping(from_ns)
    to_destination = resolve_mapping(to_ns)
    missed = g.missed_updates.pop(from_ns, {})
    if from_destination and to_destination:
        logger.info(f"Renaming {from_destination} to {to_destination}")
        try:
            g.destination_db.admin.command(
                "renameCollection", from_destination, to=to_destination, dropTarget=True
            )
        except OperationFailure as e:
            if e.code != NAMESPACE_NOT_FOUND:
                raise
            copy_from_source(to_ns)  # never reached the destination, copy it whole
        else:
            # updates that missed under the old name, fetched again under the new one
            if missed:
                apply_document_oplogs(
                    [
                        {"op": "u", "ns": to_ns, "o2": {"_id": i}}
                        for i in missed.values()
                    ]
                )
    elif from_destination:
        drop_destination(from_ns)  # renamed out of the mirror
    elif to_destination:
        copy_from_source(to_ns)  # renamed into the mirror
    forget_mapping(from_ns)


def apply_command_oplog(oplog):
    database, _ = get_database_and_collection_from_mapping(oplog["ns"])
    command = oplog["o"]
    if "dropDatabase" in command:
        for source_ns in list(g.mapping):
            if source_ns.split(".")[0] == database:
                drop_destination(source_ns)
        return
    if "renameCollection" in command:
        rename_destination(command["renameCollection"], command["to"])
        return

    key = next((key for key in COLLECTION_COMMANDS if key in command), None)
    if key is None:
        return
    source_ns = database + "." + command[key]
    destination_ns = resolve_mapping(source_ns)
    if destination_ns is None:
        return
    destination_database, destination_collection = (
        get_database_and_collection_from_mapping(destination_ns)
    )
    destination = g.destination_db[destination_database]

    if key == "create":
        if "timeseries" in command:  # buckets of a new time series
            options = {
                option: command[option]
                for option in ("timeseries", "expireAfterSeconds")
                if option in command
            }
            create_destination_collection(
                destination, destination_collection[len(BUCKETS_PREFIX) :], options
            )
        else:
            create_destination_collection(destination, destination_collection, command)
    elif key == "drop":
        drop_destination(source_ns)
    elif key in ("createIndexes", "commitIndexBuild"):
        for spec in command.get("indexes", [command]):
            logger.info(f"Creating index {spec['name']} in {destination_ns}")
            create_index_from_spec(destination[destination_collection], spec)
    elif key == "dropIndexes":
        logger.info(f"Dropping index {command['index']} in {destination_ns}")
        try:
            destination[destination_collection].drop_index(command["index"])
        except OperationFailure as e:
            if e.code != INDEX_NOT_FOUND:
                raise


def apply_view_oplog(oplog):
    source_ns = view_id(oplog)
    if oplog["op"] == "d":
        drop_destination(source_ns)
        return
    info = source_collection_info(source_ns)
    if info is not None and info["type"] == "view":
        mirror_view(source_ns, info["options"])


def apply_oplogs(oplogs):
    group = []
    for oplog in oplogs:
        if group and not (is_document_oplog(oplog) and oplog["ns"] == group[0]["ns"]):
            with_retry(apply_document_oplogs, group)
            group = []
        if is_document_oplog(oplog):
            group.append(oplog)
        elif oplog["op"] == "checkpoint":
            save_last_oplog(oplog["ts"])
            g.last_applied_oplog_timestamp = oplog["ts"]
        elif oplog["op"] == "c":
            with_retry(apply_command_oplog, oplog)
        else:
            with_retry(apply_view_oplog, oplog)
    if group:
        with_retry(apply_document_oplogs, group)


def oplog_sync():
    threading.Thread(target=oplog_monitor, daemon=True).start()

    while True:
        oplogs = [g.oplog_sync_queue.get()]
        while len(oplogs) < BATCH_SIZE:
            try:
                oplogs.append(g.oplog_sync_queue.get_nowait())
            except queue.Empty:
                break
        apply_oplogs(oplogs)


def sync():
    logger.info("Starting sync")
    mode = g.config["sync"]["mode"]

    namespaces = []  # collections holding documents, copied by full sync
    for source_ns in list(g.mapping):
        if prepare_destination(source_ns) != "collection":
            continue
        namespaces.append(source_ns)
        if g.config["sync"]["mirror_indexes"] == "true":
            mirror_indexes(source_ns)

    if mode == "full":
        full_sync(namespaces)
        return

    # start pulling before the full sync, so changes made during it are replayed after
    g.last_pulled_oplog_timestamp, outdated = start_position()
    # saved once everything before it is applied, so even an idle source records the position after a full sync
    g.oplog_sync_queue.put({"op": "checkpoint", "ts": g.last_pulled_oplog_timestamp})
    oplog_puller()
    if outdated:
        if mode == "auto":
            logger.info("Oplog outdated, starting full sync")
            full_sync(namespaces)
        else:
            logger.warning(
                "Mirroring oplog from now, earlier changes may be missing. Use auto or full mode to resync"
            )
    oplog_sync()


def main():
    g.args = parse_args()
    read_config(g.args.config)
    connect_to_mongodb()
    init_mapping()
    sync()


if __name__ == "__main__":
    main()
