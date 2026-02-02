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

package com.hortonworks.spark.sql.hive.llap;

import com.hortonworks.spark.sql.hive.llap.util.JobUtil;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.SupportsTruncate;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;

/**
 * Builds a batch writer for the HWC datasource.
 */
class HiveWarehouseWriterBuilder implements WriteBuilder, SupportsTruncate {
  protected SaveMode saveMode = SaveMode.Append;
  protected final LogicalWriteInfo logicalWriteInfo;

  /**
   * Creates a writer builder from Spark's logical write info.
   *
   * @param logicalWriteInfo Spark logical write info
   */
  public HiveWarehouseWriterBuilder(LogicalWriteInfo logicalWriteInfo) {
    this.logicalWriteInfo = logicalWriteInfo;
  }

  /**
   * Enables truncate (overwrite) mode.
   *
   * @return this builder for chaining
   */
  @Override
  public WriteBuilder truncate() {
    this.saveMode = SaveMode.Overwrite;
    return this;
  }

  /**
   * Builds the {@link Write} implementation used by Spark for batch writes.
   *
   * @return write implementation
   */
  @Override
  public Write build() {
    @SuppressWarnings("unchecked")
    Map<String, String> optionMap = (Map<String, String>) this.logicalWriteInfo.options();

    String stagingDir = HWConf.LOAD_STAGING_DIR.getFromOptionsMap(optionMap);
    String stagingDirPrefix = JobUtil.getStagingDirPrefix(stagingDir);
    Path stagingPath = new Path(stagingDirPrefix);

    Configuration hadoopConfiguration =
        SparkSession.getActiveSession().get().sparkContext().hadoopConfiguration();
    JobUtil.populateSparkConfs(hadoopConfiguration);

    HashMap<String, String> optionsCopy =
        new HashMap<>(this.logicalWriteInfo.options().asCaseSensitiveMap());

    return new HiveWarehouseWriter(
        optionsCopy,
        this.logicalWriteInfo.queryId(),
        this.logicalWriteInfo.schema(),
        stagingPath,
        hadoopConfiguration,
        this.saveMode);
  }
}
