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
import scala.Predef$;
import scala.collection.JavaConverters$;
import scala.collection.TraversableOnce;

/**
 * Utility conversions for DataFrame writer option maps.
 */
public class DataFrameWriterUtils {
  /**
   * Convert a Java map into a Scala immutable map.
   *
   * @param javaOptionMap Java option map
   * @return Scala immutable map of options
   */
  public scala.collection.immutable.Map<String, String> MapConverter(
      Map<String, String> javaOptionMap) {
    return ((TraversableOnce)
      JavaConverters$.MODULE$.mapAsScalaMapConverter(javaOptionMap).asScala())
      .toMap(Predef$.MODULE$.$conforms());
  }
}
