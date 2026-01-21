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

package org.apache.paimon.spark.execution

import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.leafnode.PaimonLeafV2CommandExec

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{NoSuchPartitionsException, ResolvedPartitionSpec}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits.TableHelper

case class PaimonDropPartitionsExec(
    table: SparkTable,
    partSpecs: Seq[ResolvedPartitionSpec],
    ignoreIfNotExists: Boolean,
    purge: Boolean,
    refreshCache: () => Unit)
  extends PaimonLeafV2CommandExec {

  override protected def run(): Seq[InternalRow] = {
    val partitionSchema = table.asPartitionable.partitionSchema()
    val (partialPartSpecs, fullPartSpecs) =
      partSpecs.partition(_.ident.numFields != partitionSchema.length)

    val (existsPartIdents, nonExistsPartIdents) =
      fullPartSpecs.map(_.ident).partition(table.partitionExists)

    if (nonExistsPartIdents.nonEmpty && !ignoreIfNotExists) {
      throw new NoSuchPartitionsException(
        table.name(),
        nonExistsPartIdents,
        table.asPartitionable.partitionSchema())
    }

    val allExistsPartIdents = existsPartIdents ++ partialPartSpecs.flatMap(expendPartialSpec)

    val isTableAltered: Boolean = if (allExistsPartIdents.nonEmpty) {
      // askwang-todo: 按批次 drop partitions
      allExistsPartIdents
        .map(
          partIdents =>
            if (purge) table.purgePartition(partIdents) else table.dropPartition(partIdents))
        .reduce(_ || _)
    } else {
      false
    }

    if (isTableAltered) {
      refreshCache()
    }
    Seq.empty
  }

  private def expendPartialSpec(partialSpec: ResolvedPartitionSpec): Seq[InternalRow] = {
    table.listPartitionIdentifiers(partialSpec.names.toArray, partialSpec.ident)
  }

  override def output: Seq[Attribute] = Seq.empty
}
