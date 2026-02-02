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

/**
 * Simple column metadata holder used by HWC.
 */
public class Column {
  private String name;
  private String dataType;
  private String comment;
  private boolean hasDefaultValue = false;

  /**
   * Creates an empty column definition.
   */
  public Column() {
  }

  /**
   * Creates a column definition without a comment.
   *
   * @param name column name
   * @param dataType column data type
   */
  public Column(String name, String dataType) {
    this.name = name;
    this.dataType = dataType;
  }

  /**
   * Creates a column definition with a comment.
   *
   * @param name column name
   * @param dataType column data type
   * @param comment column comment
   */
  public Column(String name, String dataType, String comment) {
    this.name = name;
    this.dataType = dataType;
    this.comment = comment;
  }

  /**
   * Returns the column name.
   *
   * @return column name
   */
  public String getName() {
    return name;
  }

  /**
   * Sets the column name.
   *
   * @param name column name
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Returns the data type.
   *
   * @return column data type
   */
  public String getDataType() {
    return dataType;
  }

  /**
   * Sets the data type.
   *
   * @param dataType column data type
   */
  public void setDataType(String dataType) {
    this.dataType = dataType;
  }

  /**
   * Returns the comment.
   *
   * @return column comment
   */
  public String getComment() {
    return comment;
  }

  /**
   * Sets the comment.
   *
   * @param comment column comment
   */
  public void setComment(String comment) {
    this.comment = comment;
  }

  /**
   * Marks the column as having a default value.
   *
   * @param hasDefaultValue {@code true} when the column has a default value
   */
  public void setDefault(boolean hasDefaultValue) {
    this.hasDefaultValue = hasDefaultValue;
  }

  /**
   * Indicates whether the column does not have a default value.
   *
   * @return {@code true} if the column has no default value
   */
  public boolean isNotDefault() {
    return !hasDefaultValue;
  }

  @Override
  public String toString() {
    return "Column{name='" + name + '\'' +
        ", dataType='" + dataType + '\'' +
        ", comment='" + comment + '\'' +
        ", hasDefaultValue='" + hasDefaultValue + '\'' +
        '}';
  }
}
