/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark.commands

import org.apache.paimon.spark.catalyst.analysis.expressions.ExpressionHelper
import org.apache.paimon.spark.schema.SparkSystemColumns.ROW_KIND_COL
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.PrimaryKeyTableUtils.validatePKUpsertDeletable
import org.apache.paimon.table.sink.CommitMessage
import org.apache.paimon.table.source.DataSplit
import org.apache.paimon.types.RowKind
import org.apache.paimon.utils.InternalRowPartitionComputer

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.PaimonUtils.createDataset
import org.apache.spark.sql.catalyst.expressions.{And, Expression, Not}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, SupportsSubquery}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.functions.lit

import scala.collection.JavaConverters._

case class DeleteFromPaimonTableCommand(
    relation: DataSourceV2Relation,
    override val table: FileStoreTable,
    condition: Expression)
  extends PaimonRowLevelCommand
  with ExpressionHelper
  with SupportsSubquery {

  override def run(sparkSession: SparkSession): Seq[Row] = {

    val commit = table.newBatchWriteBuilder().newCommit()
    if (condition == null || condition == TrueLiteral) {
      commit.truncateTable()
    } else {
      val (partitionCondition, otherCondition) = splitPruePartitionAndOtherPredicates(
        condition,
        table.partitionKeys().asScala.toSeq,
        sparkSession.sessionState.conf.resolver)

      val partitionPredicate = if (partitionCondition.isEmpty) {
        None
      } else {
        try {
          convertConditionToPaimonPredicate(
            partitionCondition.reduce(And),
            relation.output,
            table.schema.logicalPartitionType())
        } catch {
          case _: Throwable =>
            None
        }
      }

      if (
        otherCondition.isEmpty && partitionPredicate.nonEmpty && !table
          .coreOptions()
          .deleteForceProduceChangelog()
      ) {
        val matchedPartitions =
          table.newSnapshotReader().withPartitionFilter(partitionPredicate.get).partitions().asScala
        val rowDataPartitionComputer = new InternalRowPartitionComputer(
          table.coreOptions().partitionDefaultName(),
          table.schema().logicalPartitionType(),
          table.partitionKeys.asScala.toArray,
          table.coreOptions().legacyPartitionName()
        )
        val dropPartitions = matchedPartitions.map {
          partition => rowDataPartitionComputer.generatePartValues(partition).asScala.asJava
        }
        if (dropPartitions.nonEmpty) {
          commit.truncatePartitions(dropPartitions.asJava)
        } else {
          writer.commit(Seq.empty)
        }
      } else {
        val commitMessages = if (usePKUpsertDelete()) {
          performPrimaryKeyDelete(sparkSession)
        } else {
          performNonPrimaryKeyDelete(sparkSession)
        }
        writer.commit(commitMessages)
      }
    }

    Seq.empty[Row]
  }

  private def usePKUpsertDelete(): Boolean = {
    try {
      validatePKUpsertDeletable(table)
      true
    } catch {
      case _: UnsupportedOperationException => false
    }
  }

  private def performPrimaryKeyDelete(sparkSession: SparkSession): Seq[CommitMessage] = {
    val df = createDataset(sparkSession, Filter(condition, relation))
      .withColumn(ROW_KIND_COL, lit(RowKind.DELETE.toByteValue))
    writer.write(df)
  }

  private def performNonPrimaryKeyDelete(sparkSession: SparkSession): Seq[CommitMessage] = {
    // 读取并过滤 ManifestFileMeta 和 ManifestEntry，判断要读取的 ManifestEntry（即 DataFile），属于 plan 计划生成阶段
    // Step1: the candidate data splits which are filtered by Paimon Predicate.
    val candidateDataSplits: Seq[DataSplit] = findCandidateDataSplits(condition, relation.output)

    // convertToSparkDataFileMeta，将 DataSplit 转为 SparkDataFileMeta
    val dataFilePathToMeta: Map[String, SparkDataFileMeta] = candidateFileMap(candidateDataSplits)

    if (deletionVectorsEnabled) {
      // Step2: collect all the deletion vectors that marks the deleted rows.
      val deletionVectors = collectDeletionVectors(
        candidateDataSplits,
        dataFilePathToMeta,
        condition,
        relation,
        sparkSession)

      // Step3: update the touched deletion vectors and index files
      writer.persistDeletionVectors(deletionVectors)
    } else {
      // 将 candidateDataSplits 下推到 Relation 的 scan 阶段，DataSet 方式 scan，通过元数据列 FILE_PATH_COLUMN 拿到关联的文件路径
      // relation 是全表的数据
      // Step2: extract out the exactly files, which must have at least one record to be updated.
      val touchedFilePaths: Array[String] =
        findTouchedFiles(candidateDataSplits, condition, relation, sparkSession)

      // convertToDataSplits，将 SparkDataFileMeta 转为 DataSplit
      // Step3: the smallest range of data files that need to be rewritten.
      val (touchedFiles: Array[SparkDataFileMeta], newRelation: LogicalPlan) =
        extractFilesAndCreateNewScan(touchedFilePaths, dataFilePathToMeta, relation)

      // newRelation 只包含 touchedFile 的 relation，属于 condition 范围内的 split
      // Step4: build a dataframe that contains the unchanged data, and write out them.
      val toRewriteScanRelation: Filter = Filter(Not(condition), newRelation)
      var data: Dataset[Row] = createDataset(sparkSession, toRewriteScanRelation)
      if (coreOptions.rowTrackingEnabled()) {
        data = selectWithRowLineage(data)
      }

      // only write new files, should have no compaction
      val addCommitMessage: Seq[CommitMessage] = writer.writeOnly().withRowLineage().write(data)

      // Step5: convert the deleted files that need to be written to commit message.
      val deletedCommitMessage: Seq[CommitMessage] = buildDeletedCommitMessage(touchedFiles)

      addCommitMessage ++ deletedCommitMessage
    }
  }
}
