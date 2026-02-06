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

import org.apache.paimon.spark.PaimonHiveTestBase

import org.apache.spark.sql.internal.SQLConf

/** only spark test， not related to paimon. */
class AskwangSparkTest extends PaimonHiveTestBase {

  test("create table/create table like/create table as select") {
    withTable("tb") {
      withSQLConf(
        SQLConf.PLAN_CHANGE_LOG_LEVEL.key -> "INFO",
        SQLConf.DATETIME_JAVA8API_ENABLED.key -> "true") {
        sql("CREATE TABLE `tb1`(i INT, dt TIMESTAMP) USING parquet")
        sql("show create table tb1").show(false)

         sql("CREATE TABLE `tb2` like `tb1`")
      }
    }
  }
}
