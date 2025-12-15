# core
- [ ] file scan
- [ ] file write
- [ ] merge-engine
- [ ] lsm sorted-run
- [x] manifest push-down
- [ ] stat info
- [ ] index read/write
- [ ] changelog procedure
- [ ] deletion vector

# procedure
- [ ] compact
- [ ] compact_manifest
- [ ] expire_snapshots
- [ ] expire_partitions
  - [x] PartitionExpireStrategy CUSTOM 策略
  - [ ] PartitionLoader/PartitionHandler/PartitionPredicate
- [ ] remove_orphan_files 
- [ ] expire_tags
- [ ] create_tag/rename_tag/replace_tag/delete_tag
- [ ] rollback/rollback_to_timestamp/rollback_to/watermark
- [ ] purge_files
- [ ] migrate_table/migrate_database
- [ ] repair
- [ ] create_branch/delete_branch/fast_forward
- [ ] reset_consumer/clean_consumers
- [ ] mark_partition_done
- [ ] alter_view_dialect
- [ ] create_function/alter_function/drop_function
- [ ] remove_un-existing_files

# sql
- [ ] insert into/overwrite
- [ ] delete from
  - [x] append table
- [ ] update table
- [ ] drop partition
- [ ] merge into
- [ ] truncate table

# basic
- [x] Predicate/PredicateBuilder (PredicateTest/PredicateBuilderTest)
- [x] BinaryString/BinaryRow/BinaryRowWriter (BinaryRowTest)
- [x] InternalRowPartitionComputer (InternalRowPartitionComputerTest)
- [ ] CastExecutors
- [ ] AbstractFileStore 和 AbstractFileStoreTable
  - [ ] KeyValueFieldsExtractor
  - [ ] KeyValueFileStore keyType/valueType/partitionType/bucketKeyType