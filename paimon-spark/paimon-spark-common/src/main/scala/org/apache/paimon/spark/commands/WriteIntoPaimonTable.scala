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

import org.apache.paimon.CoreOptions.DYNAMIC_PARTITION_OVERWRITE
import org.apache.paimon.options.Options
import org.apache.paimon.spark._
import org.apache.paimon.spark.catalyst.analysis.expressions.ExpressionHelper
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.CommitMessage

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.RunnableCommand

import scala.collection.JavaConverters._

/** Used to write a [[DataFrame]] into a paimon table. */
case class WriteIntoPaimonTable(
    override val originTable: FileStoreTable,
    saveMode: SaveMode,
    _data: DataFrame,
    options: Options)
  extends RunnableCommand
  with ExpressionHelper
  with SchemaHelper
  with Logging {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val data = mergeSchema(sparkSession, _data, options)

    // 解析写入模式
    // spark.sql.sources.partitionOverwriteMode=static
    val (dynamicPartitionOverwriteMode, overwritePartition) = parseSaveMode()

    // paimon 的参数 dynamic-partition-overwrite，默认为 true
    // 默认值是不管用的，最终值还是由解析后 dynamicPartitionOverwriteMode 值决定
    // use the extra options to rebuild the table object
    updateTableWithOptions(
      Map(DYNAMIC_PARTITION_OVERWRITE.key -> dynamicPartitionOverwriteMode.toString))

    val writer = PaimonSparkWriter(table)

    // 注意，overwritePartition == null 不等于 overwritePartition.size == 0
    // 也就是说对于 OverWrite 返回的 Map.empty[String, String] 只是 size=0，不是 null
    // 所以 withOverwrite 会触发，这也为后续 static 模式创建空的 paritionPredicate 埋下伏笔
    // 空 paritionPredicate 会拿到所有 partitions 值，然后标记为 DELETE 状态，也就是所谓静态模式的的全分区 drop
    if (overwritePartition != null) {
      writer.writeBuilder.withOverwrite(overwritePartition.asJava)
    }
    val commitMessages: Seq[CommitMessage] = writer.write(data)
    writer.commit(commitMessages)

    Seq.empty
  }

  private def parseSaveMode(): (Boolean, Map[String, String]) = {
    var dynamicPartitionOverwriteMode = false
    // null 和 Map.empty[String, String] 是两种状态
    val overwritePartition = saveMode match {
      case InsertInto => null
      case Overwrite(filter) =>
        if (filter.isEmpty) {
          Map.empty[String, String]
        } else if (isTruncate(filter.get)) {
          Map.empty[String, String]
        } else {
          convertPartitionFilterToMap(filter.get, table.schema.logicalPartitionType())
        }
      case DynamicOverWrite =>
        dynamicPartitionOverwriteMode = true
        Map.empty[String, String]
      case _ =>
        throw new UnsupportedOperationException(s" This mode is unsupported for now.")
    }
    (dynamicPartitionOverwriteMode, overwritePartition)
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): LogicalPlan =
    this.asInstanceOf[WriteIntoPaimonTable]
}
