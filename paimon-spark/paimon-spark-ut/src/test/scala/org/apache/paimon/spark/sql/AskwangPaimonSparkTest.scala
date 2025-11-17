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

import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.scheduler.{SparkListener, SparkListenerStageSubmitted}
import org.apache.spark.sql.Row

import scala.jdk.CollectionConverters._

/** paimon spark test. */
class AskwangPaimonSparkTest extends PaimonSparkTestBase {

  test("String hour with int type not partition push down") {

    println(sparkVersion)

    sql(s"""
           |CREATE TABLE T (id STRING, hour STRING, appid string)
           |TBLPROPERTIES ('primary-key'='id,hour', 'bucket'='2')
           | PARTITIONED BY (hour)
           |""".stripMargin)

    sql(" insert into T values ('1', '15', '004');")
    sql(" insert into T values ('1', '16', '004');")
    sql(" insert into T values ('1', '17', '003');")
    sql(" insert into T values ('2', '17', '004');")

    //    sql("select * from T where hour=17 and appid ='004'").show(false)
    sql("select * from T where hour='17' and appid ='004'").show(false)
  }

  test(s"custom pk and bucket prop") {
    val hasPk = true
    val bucket = 4
    val prop =
      if (hasPk) s"'primary-key'='a,b,dt,hh', 'bucket' = '$bucket' "
      else if (bucket != -1) s"'bucket-key'='a,b', 'bucket' = '$bucket' "
      else "'write-only'='true'"

    spark.sql(s"""
                 |CREATE TABLE T (a VARCHAR(10), b CHAR(10),c BIGINT,dt LONG,hh VARCHAR(4))
                 |PARTITIONED BY (dt, hh)
                 |TBLPROPERTIES ($prop)
                 |""".stripMargin)

    spark.sql("INSERT INTO T VALUES('a','b',1,20230816,'1133')")
    spark.sql("INSERT INTO T VALUES('a','b',2,20230817,'1134')")
    spark.sql("INSERT INTO T VALUES('a','b',1,20230816,'1132')")
    spark.sql("INSERT INTO T VALUES('a','b',1,20230816,'1134')")
    spark.sql("INSERT INTO T VALUES('a','b',2,20230817,'1132')")
    spark.sql("INSERT INTO T VALUES('a','b',2,20230817,'1133')")
    spark.sql("INSERT INTO T VALUES('a','b',2,20240101,'00')")
    spark.sql("INSERT INTO T VALUES('a','b',2,20240102,'00')")

    spark.sql("show partitions T ").show(false)

    spark.sql("select * from `T$buckets`").show(false)
  }

  test("Paimon Procedure: test aware-bucket compaction read parallelism") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, value STRING)
                 |TBLPROPERTIES ('primary-key'='id', 'bucket'='3', 'write-only'='true')
                 |""".stripMargin)

    val table = loadTable("T")
    for (i <- 1 to 10) {
      sql(s"INSERT INTO T VALUES ($i, '$i')")
    }
    assertResult(10)(table.snapshotManager().snapshotCount())

    val buckets = table.newSnapshotReader().bucketEntries().asScala.map(_.bucket()).distinct.size
    assertResult(3)(buckets)

    val taskBuffer = scala.collection.mutable.ListBuffer.empty[Int]
    val listener = new SparkListener {
      override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
        taskBuffer += stageSubmitted.stageInfo.numTasks
      }
    }

    try {
      spark.sparkContext.addSparkListener(listener)

      // spark.default.parallelism cannot be change in spark session
      // sparkParallelism is 2, bucket is 3, use 2 as the read parallelism
      spark.conf.set("spark.sql.shuffle.partitions", 2)
      spark.sql("CALL sys.compact(table => 'T')")

      // sparkParallelism is 5, bucket is 3, use 3 as the read parallelism
      spark.conf.set("spark.sql.shuffle.partitions", 5)
      spark.sql("CALL sys.compact(table => 'T')")

      assertResult(Seq(2, 3))(taskBuffer)
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
  }

  test("system table: sort tags table") {
    spark.sql(s"""
                 |CREATE TABLE T (id STRING, name STRING)
                 |USING PAIMON
                 |""".stripMargin)

    spark.sql(s"INSERT INTO T VALUES(1, 'a')")

    spark.sql("CALL paimon.sys.create_tag(table => 'test.T', tag => '2024-10-02')")
    spark.sql("CALL paimon.sys.create_tag(table => 'test.T', tag => '2024-10-01')")
    spark.sql("CALL paimon.sys.create_tag(table => 'test.T', tag => '2024-10-04')")
    spark.sql("CALL paimon.sys.create_tag(table => 'test.T', tag => '2024-10-03')")

    checkAnswer(
      spark.sql("select tag_name from `T$tags`"),
      Row("2024-10-01") :: Row("2024-10-02") :: Row("2024-10-03") :: Row("2024-10-04") :: Nil)
  }

  test("system table: sort partitions table") {
    spark.sql(s"""
                 |CREATE TABLE T (a INT, b STRING,dt STRING,hh STRING)
                 |PARTITIONED BY (dt, hh)
                 |TBLPROPERTIES ('primary-key'='a,dt,hh', 'bucket' = '3')
                 |""".stripMargin)

    spark.sql("INSERT INTO T VALUES(1, 'a', '2024-10-10', '01')")
    spark.sql("INSERT INTO T VALUES(3, 'c', '2024-10-10', '23')")
    spark.sql("INSERT INTO T VALUES(2, 'b', '2024-10-10', '12')")
    spark.sql("INSERT INTO T VALUES(5, 'f', '2024-10-09', '02')")
    spark.sql("INSERT INTO T VALUES(4, 'd', '2024-10-09', '01')")

    checkAnswer(spark.sql("select count(*) from `T$partitions`"), Row(5) :: Nil)
    checkAnswer(
      spark.sql("select partition from `T$partitions`"),
      Row("[2024-10-09, 01]") :: Row("[2024-10-09, 02]") :: Row("[2024-10-10, 01]") :: Row(
        "[2024-10-10, 12]") :: Row("[2024-10-10, 23]") :: Nil
    )
  }

  test("system table: sort buckets table") {
    spark.sql(s"""
                 |CREATE TABLE T (a INT, b STRING,dt STRING,hh STRING)
                 |PARTITIONED BY (dt, hh)
                 |TBLPROPERTIES ('primary-key'='a,dt,hh', 'bucket' = '3')
                 |""".stripMargin)

    spark.sql("INSERT INTO T VALUES(1, 'a', '2024-10-10', '01')")
    spark.sql("INSERT INTO T VALUES(2, 'b', '2024-10-10', '01')")
    spark.sql("INSERT INTO T VALUES(3, 'c', '2024-10-10', '01')")
    spark.sql("INSERT INTO T VALUES(4, 'd', '2024-10-10', '01')")
    spark.sql("INSERT INTO T VALUES(5, 'f', '2024-10-10', '01')")

    checkAnswer(spark.sql("select count(*) from `T$partitions`"), Row(1) :: Nil)
    checkAnswer(
      spark.sql("select partition,bucket from `T$buckets`"),
      Row("[2024-10-10, 01]", 0) :: Row("[2024-10-10, 01]", 1) :: Row("[2024-10-10, 01]", 2) :: Nil)
  }

  test("PaimonPartitionManagement show partitions") {
    spark.sql(s"""
                 |CREATE TABLE T (a INT, b STRING,dt STRING,hh STRING)
                 |PARTITIONED BY (dt, hh)
                 |TBLPROPERTIES ('primary-key'='a,dt,hh', 'bucket' = '3')
                 |""".stripMargin)

    spark.sql("INSERT INTO T VALUES(1, 'a', '2024-10-10', '01')")
    spark.sql("INSERT INTO T VALUES(3, 'c', '2024-10-10', '23')")
    spark.sql("INSERT INTO T VALUES(2, 'b', '2024-10-10', '12')")
    spark.sql("INSERT INTO T VALUES(5, 'f', '2024-10-09', '02')")
    spark.sql("INSERT INTO T VALUES(4, 'd', '2024-10-09', '01')")

    spark.sql("show partitions T").show(false)
  }

  test("string read and write") {

    withSparkSQLConf("spark.sql.codegen.wholeStage" -> "false") {
      println(sparkVersion)
      sql(s"""
             |CREATE TABLE T (id STRING, value STRING)
             |TBLPROPERTIES ('primary-key'='id', 'bucket'='3', 'write-only'='true')
             |""".stripMargin)
      sql(s"INSERT INTO T VALUES ('a', 'a1')")
      sql(s"INSERT INTO T VALUES ('b', 'b1')")

      sql("select * from T").show(false)
    }
  }

  test("tmp: xxx") {
    println("version: " + sparkVersion)
  }
}
