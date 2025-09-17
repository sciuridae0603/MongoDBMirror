import time
import configparser
import argparse
import pymongo
import sys
import threading
import queue
import json
import math
import subprocess
from bson import Timestamp
from jsondiff import diff as jsondiff
from loguru import logger


class Flags:
    def __init__(self):
        self.not_found_in_source = None


class GlobalVariables:
    def __init__(self):
        self.args = None
        self.config = None
        self.flags = Flags()
        self.mapping = {}
        self.source_db = None
        self.destination_db = None
        self.log_collection = None

        self.full_sync_queue = queue.Queue()
        self.full_sync_total_collections = 0
        self.full_sync_left_collections = 0
        self.oplog_sync_queue = queue.Queue(maxsize=1024)
        self.last_processed_oplog_timestamp = None


g = GlobalVariables()

SYSTEM_DATABASES = ["admin", "config", "local"]
QUEUE_SKIP_INTERVAL_MAX_SIZE = 512


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


def read_config(path):
    logger.info(f"Reading configuration from {path}")
    try:
        g.config = configparser.RawConfigParser()
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
    if "*" in g.config["mapping"]:
        if g.config["mapping"]["*"] == "*":
            for database in g.source_db.list_database_names():
                if database in SYSTEM_DATABASES:
                    continue
                for collection in g.source_db[database].list_collection_names():
                    g.mapping[database + "." + collection] = database + "." + collection
        else:
            logger.error("Invalid mapping configuration for *")
            sys.exit(1)
    else:
        for key in g.config["mapping"]:
            if len(key.split(".")) < 2 or len(g.config["mapping"][key].split(".")) < 2:
                logger.error(f"Invalid mapping configuration for {key}")
                sys.exit(1)

            if key.split(".")[1] == "*":
                for collection in g.source_db[
                    key.split(".")[0]
                ].list_collection_names():
                    if collection.split(".")[0] == "system":
                        continue
                    g.mapping[key.split(".")[0] + "." + collection] = (
                        g.config["mapping"][key].split(".")[0] + "." + collection
                    )
            else:
                g.mapping[key] = g.config["mapping"][key]


def mirror_indexes(
    source_database, source_collection, destination_database, destination_collection
):
    logger.info(
        f"Mirroring indexes from {source_database}.{source_collection} to {destination_database}.{destination_collection}"
    )
    source_indexes = g.source_db[source_database][source_collection].index_information()
    destination_indexes = g.destination_db[destination_database][
        destination_collection
    ].index_information()

    for index in source_indexes:
        if index == "_id_":
            continue
        if index not in destination_indexes:
            logger.info(
                f"Creating index {index} in {destination_database}.{destination_collection}"
            )
            try:
                g.destination_db[destination_database][
                    destination_collection
                ].create_index(
                    source_indexes[index]["key"], name=index, **source_indexes[index]
                )
            except pymongo.errors.OperationFailure as e:
                logger.error(
                    f"Error creating index {index} in {destination_database}.{destination_collection}: {e}"
                )
        else:
            if (
                len(jsondiff(source_indexes[index], destination_indexes[index]).keys())
                != 0
            ):
                logger.info(
                    f"Updating index {index} in {destination_database}.{destination_collection}"
                )
                g.destination_db[destination_database][
                    destination_collection
                ].drop_index(index)

                while True:
                    if (
                        not index
                        in g.destination_db[destination_database][
                            destination_collection
                        ].index_information()
                    ):
                        break
                    time.sleep(3)

                g.destination_db[destination_database][
                    destination_collection
                ].create_index(
                    source_indexes[index]["key"], name=index, **source_indexes[index]
                )

    # Append index that mirror tool needs (used when check destination document not exists in source)
    if g.flags.not_found_in_source not in destination_indexes:

        logger.info(
            f"Creating index {g.flags.not_found_in_source} in {destination_database}.{destination_collection}"
        )
        g.destination_db[destination_database][destination_collection].create_index(
            [(g.flags.not_found_in_source, pymongo.ASCENDING)],
            name=g.flags.not_found_in_source,
        )

    logger.info(
        f"Mirroring indexes from {source_database}.{source_collection} to {destination_database}.{destination_collection} complete"
    )


def sync_worker():
    while True:
        if g.full_sync_queue.empty():
            break

        work = g.full_sync_queue.get()

        try:

            failed_documents = []

            source_database = work["source_database"]
            source_collection = work["source_collection"]
            destination_database = work["destination_database"]
            destination_collection = work["destination_collection"]

            count = 0
            total = g.source_db[source_database][source_collection].count_documents({})

            logger.info(
                f"Syncing {source_database}.{source_collection} to {destination_database}.{destination_collection}"
            )

            if g.config["sync"]["delete_documents_not_in_source"] == "true":
                g.destination_db[destination_database][
                    destination_collection
                ].update_many(
                    {},
                    {"$set": {g.flags.not_found_in_source: True}},
                )

            source_cursor = g.source_db[source_database][source_collection].find()
            for document in source_cursor:
                try:
                    g.destination_db[destination_database][
                        destination_collection
                    ].replace_one(
                        {"_id": document["_id"]},
                        document,
                        upsert=True,
                    )
                except:
                    logger.error(
                        f"Error syncing document {document['_id']} from {source_database}.{source_collection} to {destination_database}.{destination_collection}, appending to failed documents list"
                    )
                    failed_documents.append(document)

                count += 1

                if count % 1000 == 0:
                    logger.info(
                        f"Syncing {source_database}.{source_collection} to {destination_database}.{destination_collection} ({count}/{total} documents)"
                    )

            if g.config["sync"]["delete_documents_not_in_source"] == "true":
                g.destination_db[destination_database][
                    destination_collection
                ].delete_many({g.flags.not_found_in_source: True})
                g.destination_db[destination_database][
                    destination_collection
                ].update_many(
                    {},
                    {"$unset": {g.flags.not_found_in_source: ""}},
                )

            if len(failed_documents) > 0:
                logger.info(
                    f"Processing {source_database}.{source_collection} to {destination_database}.{destination_collection} failed documents"
                )

                for document in failed_documents:
                    try:
                        g.destination_db[destination_database][
                            destination_collection
                        ].replace_one(
                            {"_id": document["_id"]},
                            document,
                            upsert=True,
                        )
                    except:
                        logger.error(
                            f"Still error syncing document {document['_id']} from {source_database}.{source_collection} to {destination_database}.{destination_collection}"
                        )

            g.full_sync_left_collections -= 1
            logger.info(
                f"Syncing {source_database}.{source_collection} to {destination_database}.{destination_collection} complete ({count} documents)"
            )
        except Exception as e:
            g.full_sync_queue.put(work)
            logger.error(
                f"Error syncing {source_database}.{source_collection} to {destination_database}.{destination_collection}, put back to queue"
            )
            logger.error(e)


def full_sync():
    logger.info("Starting full sync")

    g.full_sync_total_collections = len(g.mapping)
    g.full_sync_left_collections = len(g.mapping)
    for collection in g.mapping:
        source_database, source_collection = get_database_and_collection_from_mapping(
            collection
        )
        destination_database, destination_collection = (
            get_database_and_collection_from_mapping(g.mapping[collection])
        )
        g.full_sync_queue.put(
            {
                "source_database": source_database,
                "source_collection": source_collection,
                "destination_database": destination_database,
                "destination_collection": destination_collection,
            }
        )

    for i in range(0, int(g.config["sync"]["threads"])):
        t = threading.Thread(target=sync_worker)
        t.daemon = True
        t.start()

    while g.full_sync_left_collections > 0:
        time.sleep(0.1)

    logger.info("Full sync complete")


def read_last_oplog():
    # check log file exists g.config["sync"]["last_optime_file"]
    try:
        with open(g.config["sync"]["last_optime_file"], "r", encoding="utf-8") as f:
            oplog = json.loads(f.read())
            g.last_processed_oplog_timestamp = oplog["ts"]
    except Exception as e:
        logger.error(f"Error reading last optime: {e}")
        return None


def save_last_oplog(oplog):
    try:
        with open(g.config["sync"]["last_optime_file"], "w", encoding="utf-8") as f:
            f.write(json.dumps({"ts": oplog["ts"]}))
    except Exception as e:
        logger.error(f"Error saving last optime: {e}")


def get_oplogs(start_time, end_time):
    start_time = convert_milliseconds_to_bson_timestamp(start_time)
    end_time = convert_milliseconds_to_bson_timestamp(end_time)

    try:
        oplogs = list(
            g.source_db["local"]["oplog.rs"].find(
                {
                    "op": {"$in": ["i", "u", "d", "n"]},
                    "ts": {
                        "$gte": start_time,
                        "$lte": end_time,
                    },
                },
            )
        )
        for oplog in oplogs:
            oplog["ts"] = convert_bson_timestamp_to_milliseconds(oplog["ts"])

        return oplogs
    except Exception as e:
        logger.error(f"Error reading oplog: {e}")
        return None


def convert_bson_timestamp_to_milliseconds(timestamp):
    return timestamp.time * 1000 + timestamp.inc


def convert_milliseconds_to_bson_timestamp(milliseconds):
    return Timestamp(int(milliseconds / 1000), int(milliseconds % 1000))


def oplog_outdated():
    if g.last_processed_oplog_timestamp == None:
        g.last_processed_oplog_timestamp = time.time() * 1000
        return True

    oplogs = get_oplogs(
        g.last_processed_oplog_timestamp, g.last_processed_oplog_timestamp
    )

    for oplog in oplogs:
        if oplog["ts"] == g.last_processed_oplog_timestamp:
            return False

    return True


def oplog_puller():
    def oplog_puller_worker():
        while True:
            start_time = g.last_processed_oplog_timestamp
            end_time = min(
                g.last_processed_oplog_timestamp + (1000 * 60 * 60), time.time() * 1000
            )
            # Backtracking means the end time is less than 1.5 times the interval ago
            backtracking = end_time < time.time() * 1000 - (
                int(g.config["sync"]["oplog_pull_interval"]) * 1.5 * 1000
            )
            logger.info(
                f"Getting oplogs from {str(start_time)} to {str(end_time)}, last processed timestamp is {str(g.last_processed_oplog_timestamp)}"
            )

            oplogs = get_oplogs(start_time, end_time)

            count = 0
            for oplog in oplogs:
                if (
                    oplog["ts"] > g.last_processed_oplog_timestamp
                    and oplog["ns"] in g.mapping
                ):
                    count += 1
                    g.oplog_sync_queue.put(oplog)

            if backtracking and count == 0:
                logger.info(
                    f"No oplogs found in backtracking. Advancing timestamp to {str(end_time)}"
                )
                g.last_processed_oplog_timestamp = end_time
                continue
            else:
                g.last_processed_oplog_timestamp = oplogs[-1]["ts"]
                logger.info(
                    f"Got {count} oplogs, updating last processed timestamp to {str(g.last_processed_oplog_timestamp)}"
                )
                save_last_oplog(oplogs[-1])

            if (
                not backtracking
                or g.oplog_sync_queue.qsize() > QUEUE_SKIP_INTERVAL_MAX_SIZE
            ):
                time.sleep(int(g.config["sync"]["oplog_pull_interval"]))

    t = threading.Thread(target=oplog_puller_worker)
    t.daemon = True
    t.start()


def oplog_monitor():
    while True:
        if g.config.has_section("monitor") and g.config["monitor"]["enabled"] == "true":

            mirror_key = g.config["monitor"]["mirror_key"]
            current_time = time.time() * 1000
            oplog_queue_size = g.oplog_sync_queue.qsize()
            last_processed_oplog_timestamp = (
                str(g.last_processed_oplog_timestamp)
                if g.last_processed_oplog_timestamp
                else None
            )

            if g.config["monitor"]["type"] == "database":
                if g.log_collection:
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
                        args = (
                            command.split(" ")
                            + [
                                str(mirror_key),
                                str(int(current_time)),
                                str(oplog_queue_size),
                                str(last_processed_oplog_timestamp) if last_processed_oplog_timestamp is not None else "",
                            ]
                        )
                        process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                        stdout, stderr = process.communicate()
                        if stdout:
                            logger.info(f"Monitor script output: {stdout.decode('utf-8')}")
                        if stderr:
                            logger.error(f"Monitor script error: {stderr.decode('utf-8')}")
                        process.wait()
                    except FileNotFoundError:
                        logger.error(f"Monitor script not found at {command}")
                    except Exception as e:
                        logger.error(f"Error executing monitor script: {e}")

        time.sleep(1)


def oplog_sync():
    monitor_thread = threading.Thread(target=oplog_monitor)
    monitor_thread.daemon = True
    monitor_thread.start()

    while True:
        if g.oplog_sync_queue.empty():
            time.sleep(0.1)
            continue

        oplog = g.oplog_sync_queue.get()

        source_database_collection = oplog["ns"]
        if source_database_collection not in g.mapping:
            continue

        source_database, source_collection = get_database_and_collection_from_mapping(
            source_database_collection
        )
        destination_database, destination_collection = (
            get_database_and_collection_from_mapping(
                g.mapping[source_database_collection]
            )
        )

        if oplog["op"] == "i":
            g.destination_db[destination_database][destination_collection].replace_one(
                {"_id": oplog["o"]["_id"]},
                oplog["o"],
                upsert=True,
            )
        elif oplog["op"] == "u":
            # hard to parse update operation
            # so just fetch the document from source and replace it in destination
            document = g.source_db[source_database][source_collection].find_one(
                {"_id": oplog["o2"]["_id"]}
            )
            if not document:
                continue
            g.destination_db[destination_database][destination_collection].replace_one(
                {"_id": document["_id"]},
                document,
                upsert=True,
            )
        elif oplog["op"] == "d":
            g.destination_db[destination_database][destination_collection].delete_one(
                {"_id": oplog["o"]["_id"]}
            )


def sync():
    logger.info("Starting sync")

    if g.config["sync"]["mirror_indexes"] == "true":
        logger.info("Mirroring indexes")

        for collection in g.mapping:
            source_database, source_collection = (
                get_database_and_collection_from_mapping(collection)
            )
            destination_database, destination_collection = (
                get_database_and_collection_from_mapping(g.mapping[collection])
            )

            mirror_indexes(
                source_database,
                source_collection,
                destination_database,
                destination_collection,
            )

        logger.info("Indexes mirroring complete")

    if g.config["sync"]["mode"] != "full":
        read_last_oplog()

    if g.config["sync"]["mode"] == "auto":
        logger.info("Checking oplog is outdated or not")
        if oplog_outdated():
            logger.info("Oplog outdated, starting full sync")
            oplog_puller()
            full_sync()
        else:
            logger.info("Oplog is up to date, starting oplog sync")
            oplog_puller()
        oplog_sync()
    if g.config["sync"]["mode"] == "full":
        full_sync()
    if g.config["sync"]["mode"] == "oplog":
        oplog_puller()
        oplog_sync()


def main():
    g.args = parse_args()
    read_config(g.args.config)
    connect_to_mongodb()
    init_mapping()
    sync()


if __name__ == "__main__":
    main()
