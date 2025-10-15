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

package org.apache.paimon.spark.procedure

import org.apache.paimon.fs.Path
import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.sql.Row

class CleanEmptyDirectoriesProcedureAskwangTest extends PaimonSparkTestBase {

  test("Paimon procedure: purge files test") {
    spark.sql(s"""
                 |CREATE TABLE T (id STRING, name STRING)
                 |USING PAIMON
                 |""".stripMargin)

    spark.sql("insert into T select '1', 'aa'");
    checkAnswer(spark.sql("select * from test.T"), Row("1", "aa") :: Nil)

    val table = loadTable("T")
    val location: Path = table.location()
    val fileIO = table.fileIO()
    val day = new Path(location, "day=2025-10-14")
    val bucket10 = new Path(day, "bucket-10")
    val bucket11 = new Path(day, "bucket-11")
    fileIO.checkOrMkdirs(bucket10)
    fileIO.checkOrMkdirs(bucket11)

    val fileInBucket11 = new Path(bucket11, "random-file.txt")
    table.fileIO().writeFile(fileInBucket11, "random content", true)
    val statuses = table.fileIO().listDirectories(location)
    statuses.foreach(println(_))

    val deletedDirectories = table.cleanEmptyDirectoriesAskwang()
    println(deletedDirectories)
  }

  /**
   * /day=2025-10-15/hour=00/ /bucket-0 --> not empty /day=2025-10-15/hour=01/ /bucket-0 --> empty
   * /day=2025-10-15/hour=02/ --> empty /day=2025-10-16/ --> empty
   */
  test("xxx") {
    spark.sql(s"""
                 |CREATE TABLE T (k STRING, day STRING, hour STRING)
                 |TBLPROPERTIES ('primary-key'='k,day,hour', 'bucket'='1')
                 | PARTITIONED BY (day, hour)
                 |""".stripMargin)
    spark.sql("insert into T values(1,'2025-10-15', 00),(2,'2025-10-15', 00)")

    val table = loadTable("T")
    val location: Path = table.location()
    val fileIO = table.fileIO()

    val emptyBucketPath1 = new Path(location, "day=2025-10-15/hour=00/bucket-1")
    fileIO.mkdirs(emptyBucketPath1)

    val emptyBucketPath2 = new Path(location, "day=2025-10-15/hour=01/bucket-0")
    fileIO.mkdirs(emptyBucketPath2)

    val emptyHourPath = new Path(location, "day=2025-10-15/hour=02")
    fileIO.mkdirs(emptyHourPath)

    val emptyDayPath = new Path(location, "day=2025-10-16")
    fileIO.mkdirs(emptyDayPath)

    sql("select *,__paimon_file_path from T").show(false)

    val deletedDirectories = table.cleanEmptyDirectoriesAskwang()
  }
}
