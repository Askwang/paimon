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

package org.apache.paimon.spark.sql

import org.apache.paimon.schema.{Schema, SchemaManager, TableSchema}
import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.sql.execution.QueryExecution

import java.util.Optional

/** paimon spark test. */
class AskwangPaimonSQLTest extends PaimonSparkTestBase {

  println(sparkVersion)

  // ----------------------------------- read and write ---------------------------------

  test("[read/write] insert into non-partition/no-bucket/append table") {

    withSparkSQLConf("spark.sql.planChangeLog.level" -> "TRACE") {
      sql(s"""
             |CREATE TABLE T (id int, name string)
             |TBLPROPERTIES ('bucket-key' = 'id', 'bucket' = '1', 'file.format'='parquet')
             |""".stripMargin)

      val df = sql("insert into T values (1,'zhangsan'),(2, 'lisi')")

      // AskwangUtils.printNumberTree(df.queryExecution)
      sql("select * from T").show(false)
    }
  }

  test("cast(hour as int) not push down") {

    sql(s"""
           |CREATE TABLE T (id STRING, hour STRING, appid string, day string)
           |TBLPROPERTIES ('primary-key'='id,hour,day', 'bucket'='2')
           | PARTITIONED BY (hour,day)
           |""".stripMargin)

    sql(" insert into T values ('1', '15', '004', '2021');")
    sql(" insert into T values ('1', '16', '004', '2021');")
    sql(" insert into T values ('1', '17', '003', '2025');")
    sql(" insert into T values ('2', '17', '004', '2025');")

    // sql("select * from T where hour=17 and appid ='004'").show(false)
    // sql("select * from T where hour='17' and appid ='004' and day = '2025' ").show(false)
    sql("select * from T where hour='17' and id = '2' and day = '2025' ").show(false)
  }

  test("alter table: drop partitions") {
    println(sparkVersion)

    sql(s"""
           |CREATE TABLE T (id STRING, hour STRING, appid string, day string)
           |TBLPROPERTIES ('primary-key'='id,hour,day', 'bucket'='2')
           | PARTITIONED BY (hour,day)
           |""".stripMargin)

    // drop partition (day='2025-01-15')，删除 2 个分区
    // drop partition (hour='16')，删除 2 个分区
    // drop partition (hour='17' ,day='2026-01-16')，删除 1 个分区
    // drop partition (hour='01'), partition(day='2026-01-01')
    sql(" insert into T values ('1', '15', '004', '2026-01-15');")
    sql(" insert into T values ('1', '16', '004', '2026-01-15');")
    sql(" insert into T values ('1', '16', '003', '2026-01-16');")
    sql(" insert into T values ('2', '17', '004', '2026-01-16');")
    sql("select * from `T$partitions`").show(false)

    val df = sql(
      "alter table T drop partition (day='2026-01-15', hour = '16'), partition (day='2026-01-16', hour = '16')")
    df.explain(true)
    // sql("select * from `T$partitions`").show(false)
    sql("show partitions T").show(false)

//    sql("alter table T drop partition (day='2025-01-16')")
//    sql("select * from `T$partitions`").show(false)
  }

  test("alter table: add partitions") {

    sql(s"""
           |CREATE TABLE T (id STRING, appid string, day string, hour STRING)
           |TBLPROPERTIES ('primary-key'='id,hour,day', 'bucket'='2')
           | PARTITIONED BY (hour,day)
           |""".stripMargin)

    // add partition 多个分区中间没有 ','
    // paimon 不会自动 commit 空的 partition，只是把分区信息同步到 hms
    sql(
      "alter table T add partition (day='2026-01-01', hour = '01') partition (day='2026-01-16', hour = '02')")

    sql("show partitions T").show(false)
  }

  test("alter table: add/change/remove table prop") {
    sql(
      s"""
         |CREATE TABLE T (id STRING, appid string, day string, hour STRING)
         |TBLPROPERTIES ('primary-key'='id,hour,day', 'bucket'='2', 'manifest.full-compaction-threshold-size'='32mb')
         | PARTITIONED BY (hour,day)
         |""".stripMargin)

    val table = loadTable("T")
    val schemaManager = table.schemaManager()

//    sql("alter table T set TBLPROPERTIES ('a' = '32m')")
//    sql("alter table T set TBLPROPERTIES ('a ' = '323m')")
//    val value: Optional[TableSchema] = schemaManager.latest()
//    println(value.get().options())

//    sql("alter table T set TBLPROPERTIES ('manifest.full-compaction-threshold-size' = '32m')")
//    sql("alter table T set TBLPROPERTIES ('manifest.full-compaction-threshold-size' = '64m')")

    printSchema(schemaManager)

    // key不存在/存在
    sql("alter table T unset TBLPROPERTIES ('manifest.full-compaction-threshold-size')")

    printSchema(schemaManager)

  }

  def printSchema(schemaManager: SchemaManager): Unit = {
    val schema = schemaManager.latest().get()
    println(schema.id() + " : " + schema.options() + " : " + schema.comment())
  }

  test("alter table: add/change/remove table comment") {
    sql(
      s"""
         |CREATE TABLE T (id STRING, appid string, day string, hour STRING)
         |TBLPROPERTIES ('primary-key'='id,hour,day', 'bucket'='2', 'manifest.full-compaction-threshold-size'='32mb')
         | PARTITIONED BY (hour,day)
         | comment 'old_comment'
         |""".stripMargin)
    val table = loadTable("T")
    val schemaManager = table.schemaManager()

    sql("alter table T set TBLPROPERTIES ('comment' = 'old_comment')")

    printSchema(schemaManager)

    sql("alter table T unset TBLPROPERTIES ('comment')")
    printSchema(schemaManager)
  }

  test("alter table: add new cols / add col position / drop cols") {
    sql(
      s"""
         |CREATE TABLE T (id STRING, appid string default '000', day string, hour STRING)
         |TBLPROPERTIES ('primary-key'='id,hour,day', 'bucket'='2')
         | PARTITIONED BY (hour,day)
         | comment 'old_comment'
         |""".stripMargin)
    val table = loadTable("T")
    val schemaManager = table.schemaManager()

    sql("alter table T add columns (c1 int comment 'c111' after id, c2 string after c1)")

    println(schemaManager.latest().get())
  }

  test("alter table: add new cols / add col position / drop cols for nested type") {
    sql(
      s"""
         |CREATE TABLE T (id STRING, v STRUCT<f1: STRING, f2: INT>)
         |TBLPROPERTIES ('primary-key'='id', 'bucket'='2')
         | comment 'old_comment'
         |""".stripMargin)
    val table = loadTable("T")
    val schemaManager = table.schemaManager()

    sql("alter table T add columns v.f3 STRING after f1")

    println(schemaManager.latest().get())
  }

  test("alter table: rename col name / change col type / change col comment / change col position") {
    sql(
      s"""
         |CREATE TABLE T (id STRING, appid string, day string, hour STRING)
         |TBLPROPERTIES ('primary-key'='id,hour,day', 'bucket'='2')
         | PARTITIONED BY (hour,day)
         | comment 'old_comment'
         |""".stripMargin)
    val table = loadTable("T")
  }

  // ----------------------------------- merge engine ---------------------------------

  test("[merge-engine] deduplicate with sequence.field") {
    sql("""
          | create table T (id string, merge_field bigint, phone string)
          | TBLPROPERTIES ('primary-key'='id', 'bucket'='1',
          | 'merge-engine'='deduplicate', 'sequence.field'='merge_field')
          |""".stripMargin)

    sql("insert into T values ('1', 1, 'a')")
    sql("insert into T values ('1', 1, 'c')")
    sql("insert into T values ('1', 0, 'b')")
  }

}

object AskwangUtils {
  def printNumberTree(qe: QueryExecution): Unit = {
    println("=== Parsed Logical Plan ===")
    println(qe.logical.numberedTreeString)

    println("=== Analyzed Logical Plan ===")
    println(qe.analyzed.numberedTreeString)

    println("=== Optimized Logical Plan ===")
    println(qe.optimizedPlan.numberedTreeString)

    println("=== Physical Plan ===")
    println(qe.sparkPlan.numberedTreeString)
  }
}
