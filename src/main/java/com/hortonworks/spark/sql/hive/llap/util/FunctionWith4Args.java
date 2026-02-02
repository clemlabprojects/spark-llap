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

package com.hortonworks.spark.sql.hive.llap.util;

/**
 * Functional interface for functions that accept four arguments.
 *
 * @param <T1> first argument type
 * @param <T2> second argument type
 * @param <T3> third argument type
 * @param <T4> fourth argument type
 * @param <R> return type
 */
@FunctionalInterface
public interface FunctionWith4Args<T1, T2, T3, T4, R> {
  /**
   * Applies the function to the provided arguments.
   *
   * @param firstArgument first argument
   * @param secondArgument second argument
   * @param thirdArgument third argument
   * @param fourthArgument fourth argument
   * @return computed result
   */
  R apply(T1 firstArgument, T2 secondArgument, T3 thirdArgument, T4 fourthArgument);
}
