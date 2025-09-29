# core
- LevelsTest 
- ManifestListTest.java 
- MergeTreeTestBase 
- CastExecutorTest 
- FieldAggregatorTest 
- SparkFilterConverterTest
- ExpireSnapshotsTest 
- ExpireSnapshotsProcedureTest 
- FileDeletionTest
  - 过期 snapshot、tag的文件删除；
  - 空bucket/partition 目录的删除
- OrphanFilesCleanTest 
- PartitionExpireTest 
- PartitionPredicateTest
- ReadWriteTableITCase 
- TestChangelogDataReadWrite 
- PartialUpdateITCase 
- MergeTreeTestBase 
- SortBufferWriteBufferTestBase 
- FileStoreCommitTest 
- KeyValueFileReadWriteTest 
  - checkRollingFiles，writer.targetFileSize()文件滚动切换
- AppendOnlyWriterTest

# sql
- DDLWithHiveCatalogTestBase