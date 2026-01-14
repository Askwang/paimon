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

package org.apache.paimon.spark

import org.apache.paimon.CoreOptions
import org.apache.paimon.partition.PartitionPredicate
import org.apache.paimon.predicate.{Predicate, PredicateBuilder, TopN}
import org.apache.paimon.spark.schema.PaimonMetadataColumn
import org.apache.paimon.table.{InnerTable, SpecialFields}
import org.apache.paimon.table.source.ReadBuilder
import org.apache.paimon.types.RowType
import org.apache.paimon.utils.Preconditions.checkState

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

trait ColumnPruningAndPushDown extends Scan with Logging {

  def table: InnerTable

  // Column pruning
  def requiredSchema: StructType

  def pushedPartitionFilters: Seq[PartitionPredicate]
  def pushedDataFilters: Seq[Predicate]
  def pushedLimit: Option[Int] = None
  def pushedTopN: Option[TopN] = None

  lazy val tableRowType: RowType = {
    val coreOptions: CoreOptions = CoreOptions.fromMap(table.options())
    if (coreOptions.rowTrackingEnabled()) {
      SpecialFields.rowTypeWithRowLineage(table.rowType())
    } else {
      table.rowType()
    }
  }

  lazy val tableSchema: StructType = SparkTypeUtils.fromPaimonRowType(tableRowType)

  final def partitionType: StructType = {
    SparkTypeUtils.toSparkPartitionType(table)
  }

  private[paimon] val (readTableRowType, metadataFields) = {
    checkState(
      requiredSchema.fields.forall(
        field =>
          tableRowType.containsField(field.name) ||
            PaimonMetadataColumn.SUPPORTED_METADATA_COLUMNS.contains(field.name)))
    val (_requiredTableFields, _metadataFields) =
      requiredSchema.fields.partition(field => tableRowType.containsField(field.name))
    val _readTableRowType =
      SparkTypeUtils.prunePaimonRowType(StructType(_requiredTableFields), tableRowType)
    (_readTableRowType, _metadataFields)
  }

  lazy val readBuilder: ReadBuilder = {
    val _readBuilder: ReadBuilder = table.newReadBuilder().withReadType(readTableRowType)
    // 由于在 PaimonBaseScanBuilder#splitPartitionPredicatesAndDataPredicates 已经拆分了 partition filter 和 data filter，
    // 这里单独注入到 ReadBuilder 中，所以在 ReadBuilderImpl#configureScan 的 scan.withFilter(filter) 的内部调用 SnapshotReaderImpl#withFilter 时，
    // splitPartitionPredicatesAndDataPredicates 就只处理 data filter。
    if (pushedPartitionFilters.nonEmpty) {
      _readBuilder.withPartitionFilter(PartitionPredicate.and(pushedPartitionFilters.asJava))
    }
    if (pushedDataFilters.nonEmpty) {
      _readBuilder.withFilter(pushedDataFilters.asJava)
    }
    pushedLimit.foreach(_readBuilder.withLimit)
    pushedTopN.foreach(_readBuilder.withTopN)
    _readBuilder.dropStats()
  }

  final def metadataColumns: Seq[PaimonMetadataColumn] = {
    metadataFields.map(field => PaimonMetadataColumn.get(field.name, partitionType))
  }

  override def readSchema(): StructType = {
    val _readSchema = StructType(
      SparkTypeUtils.fromPaimonRowType(readTableRowType).fields ++ metadataFields)
    if (!_readSchema.equals(requiredSchema)) {
      logInfo(
        s"Actual readSchema: ${_readSchema} is not equal to spark pushed requiredSchema: $requiredSchema")
    }
    _readSchema
  }

  override def description(): String = {
    val pushedPartitionFiltersStr = if (pushedPartitionFilters.nonEmpty) {
      ", PartitionFilters: [" + pushedPartitionFilters.mkString(",") + "]"
    } else {
      ""
    }
    val pushedDataFiltersStr = if (pushedDataFilters.nonEmpty) {
      ", DataFilters: [" + pushedDataFilters.mkString(",") + "]"
    } else {
      ""
    }
    s"${getClass.getSimpleName}: [${table.name}]" +
      pushedPartitionFiltersStr +
      pushedDataFiltersStr +
      pushedTopN.map(topN => s", TopN: [$topN]").getOrElse("") +
      pushedLimit.map(limit => s", Limit: [$limit]").getOrElse("")
  }
}
