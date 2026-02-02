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

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.types.StructType;

/**
 * Write adapter that creates a batch writer for HWC writes.
 */
public class HiveWarehouseWriter implements Write {
  protected final Map<String, String> optionMap;
  protected final String queryId;
  protected final StructType schema;
  protected final Path outputPath;
  protected final Configuration hadoopConfiguration;
  protected final SaveMode saveMode;

  /**
   * Create a write adapter for HWC batch writes.
   *
   * @param optionMap data source options
   * @param queryId Spark query id
   * @param schema input schema
   * @param outputPath base output path
   * @param hadoopConfiguration Hadoop configuration
   * @param saveMode Spark save mode
   */
  public HiveWarehouseWriter(Map<String, String> optionMap,
                             String queryId,
                             StructType schema,
                             Path outputPath,
                             Configuration hadoopConfiguration,
                             SaveMode saveMode) {
    this.optionMap = optionMap;
    this.queryId = queryId;
    this.schema = schema;
    this.outputPath = outputPath;
    this.hadoopConfiguration = hadoopConfiguration;
    this.saveMode = saveMode;
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public BatchWrite toBatch() {
    return new HiveWarehouseDataSourceWriter(
      optionMap,
      queryId,
      schema,
      outputPath,
      hadoopConfiguration,
      saveMode);
  }
}
