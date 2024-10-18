# MongoDB Mirror Tool

(Currently only source only support replication set)

## Usage

```bash 
python main.py -c confs/example.conf
```

## Config
```ini
[sync]
# Sync mode, available values:
# full : Mirror the data from the source to the destination
# oplog : Only mirror operations from oplog
# auto : If oplog outdated, will sync full data then switch to oplog
mode=auto
# Last optime file
last_optime_file=logs/example.optime
# Log file
log_file=logs/example.log
# Number of threads (only used in full sync)
threads=8
# Source URI
source_uri=mongodb://user:user@localhost:27001/?authMechanism=DEFAULT&authSource=admin
# Destination URI
destination_uri=mongodb://127.0.0.1:27000
# Field prefix for the flag (currently only used delete documents not in source)
flag_perfix=mongodb_mirror_
# Enable delete documents not in source
delete_documents_not_in_source=true
# Interval for pulling oplog (in seconds)
oplog_pull_interval=1
# Enable mirror indexes
mirror_indexes=true

[mapping]
# *=* : Sync all databases
# src_db_1.*=dest_db_1.* : Sync all collections in src_db_1 to dest_db_1
# src_db_2.data=dest_db_1.data_2 : Sync src_db_2.data to dest_db_1.data_2
*=*
```