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

package com.hortonworks.spark.hive.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.spark.serializer.KryoRegistrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.reflect.ScalaSignature;

/**
 * Registers Hive ACID helper classes with Kryo when they are available on the classpath.
 */
@ScalaSignature(bytes = "\u0006\u0001M2Aa\u0001\u0003\u0001\u001f!)\u0011\u0005\u0001C\u0001E!)Q\u0005\u0001C!M\t9\u0002*\u001b<f\u0003\u000eLGmS=s_J+w-[:ue\u0006$xN\u001d\u0006\u0003\u000b\u0019\tQ!\u001e;jYNT!a\u0002\u0005\u0002\t!Lg/\u001a\u0006\u0003\u0013)\tQa\u001d9be.T!a\u0003\u0007\u0002\u0017!|'\u000f^8oo>\u00148n\u001d\u0006\u0002\u001b\u0005\u00191m\\7\u0004\u0001M\u0019\u0001\u0001\u0005\f\u0011\u0005E!R\"\u0001\n\u000b\u0003M\tQa]2bY\u0006L!!\u0006\n\u0003\r\u0005s\u0017PU3g!\t9r$D\u0001\u0019\u0015\tI\"$\u0001\u0006tKJL\u0017\r\\5{KJT!!C\u000e\u000b\u0005qi\u0012AB1qC\u000eDWMC\u0001\u001f\u0003\ry'oZ\u0005\u0003Aa\u0011qb\u0013:z_J+w-[:ue\u0006$xN]\u0001\u0007y%t\u0017\u000e\u001e \u0015\u0003\r\u0002\"\u0001\n\u0001\u000e\u0003\u0011\tqB]3hSN$XM]\"mCN\u001cXm\u001d\u000b\u0003O)\u0002\"!\u0005\u0015\n\u0005%\u0012\"\u0001B+oSRDQa\u000b\u0002A\u00021\nAa\u001b:z_B\u0011Q&M\u0007\u0002])\u00111f\f\u0006\u0003a1\t\u0001#Z:pi\u0016\u0014\u0018nY:pMR<\u0018M]3\n\u0005Ir#\u0001B&ss>\u0004")
public class HiveAcidKyroRegistrator implements KryoRegistrator {
  private static final Logger LOG = LoggerFactory.getLogger(HiveAcidKyroRegistrator.class);

  /**
   * Registers Hive ACID classes for Kryo serialization if they are present.
   *
   * @param kryoInstance Kryo instance used by Spark
   */
  @Override
  public void registerClasses(Kryo kryoInstance) {
    try {
      // Register optional Hive ACID configuration wrapper when available.
      Class<?> serializableConfigurationClass =
          Class.forName("com.qubole.spark.hiveacid.util.SerializableConfiguration");
      kryoInstance.register(serializableConfigurationClass, (Serializer) new JavaSerializer());
    } catch (ClassNotFoundException exception) {
      LOG.debug("Hive acid SerializableConfiguration not found; skipping Kryo registration.");
    }
  }
}
