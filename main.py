import time
import configparser
import argparse
import pymongo
import sys
import threading
import queue
import json
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

        self.full_sync_queue = queue.Queue()
        self.full_sync_total_collections = 0
        self.full_sync_left_collections = 0
        self.oplog_sync_queue = queue.Queue()
        self.last_processed_oplog_timestamp = None


g = GlobalVariables()

SYSTEM_DATABASES = ["admin", "config", "local"]


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
            if (
                len(key.split(".")) != 2
                or len(g.config["mapping"][key].split(".")) != 2
            ):
                logger.error(f"Invalid mapping configuration for {key}")
                sys.exit(1)

            if key.split(".")[1] == "*":
                for collection in g.source_db[
                    key.split(".")[0]
                ].list_collection_names():
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

        failed_documents = []

        work = g.full_sync_queue.get()
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
            g.destination_db[destination_database][destination_collection].update_many(
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
            g.destination_db[destination_database][destination_collection].delete_many(
                {g.flags.not_found_in_source: True}
            )
            g.destination_db[destination_database][destination_collection].update_many(
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


def full_sync():
    logger.info("Starting full sync")

    g.full_sync_total_collections = len(g.mapping)
    g.full_sync_left_collections = len(g.mapping)
    for collection in g.mapping:
        g.full_sync_queue.put(
            {
                "source_database": collection.split(".")[0],
                "source_collection": collection.split(".")[1],
                "destination_database": g.mapping[collection].split(".")[0],
                "destination_collection": g.mapping[collection].split(".")[1],
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


def get_oplogs():
    try:
        oplogs = list(
            g.source_db["local"]["oplog.rs"].find(
                {
                    "ns": {"$in": list(g.mapping.keys()) + [""]},
                    "op": {"$in": ["i", "u", "d", "n"]},
                    "ts": {
                        "$gte": Timestamp(
                            int((g.last_processed_oplog_timestamp / 1000) - 60), 0
                        )
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


def oplog_outdated():
    if g.last_processed_oplog_timestamp == None:
        g.last_processed_oplog_timestamp = time.time() * 1000
        return True

    oplogs = get_oplogs()

    for oplog in oplogs:
        if oplog["ts"] == g.last_processed_oplog_timestamp:
            return False

    return True


def oplog_puller():
    def oplog_puller_worker():
        while True:
            oplogs = get_oplogs()
            for oplog in oplogs:
                if oplog["ts"] > g.last_processed_oplog_timestamp:
                    g.oplog_sync_queue.put(oplog)

            g.last_processed_oplog_timestamp = oplogs[-1]["ts"]
            save_last_oplog(oplogs[-1])

            time.sleep(int(g.config["sync"]["oplog_pull_interval"]))

    t = threading.Thread(target=oplog_puller_worker)
    t.daemon = True
    t.start()


def oplog_sync():
    while True:
        if g.oplog_sync_queue.empty():
            time.sleep(0.1)
            continue

        oplog = g.oplog_sync_queue.get()

        source_database_collection = oplog["ns"]
        if source_database_collection not in g.mapping:
            continue

        source_database = source_database_collection.split(".")[0]
        source_collection = source_database_collection.split(".")[1]
        destination_database = g.mapping[source_database_collection].split(".")[0]
        destination_collection = g.mapping[source_database_collection].split(".")[1]

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
            source_database = collection.split(".")[0]
            source_collection = collection.split(".")[1]
            destination_database = g.mapping[collection].split(".")[0]
            destination_collection = g.mapping[collection].split(".")[1]

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
