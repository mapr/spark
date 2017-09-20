/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mapr.db.spark.api.java;

import com.mapr.db.spark.impl.OJAIDocument;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class MapRDBJavaContext {
    private MapRDBJavaContext() {
        assert false;
    }

    public static SparkContextJavaFunctions maprDBSparkContext(SparkContext sparkContext) {
        return new SparkContextJavaFunctions(sparkContext);
    }

    public static SparkContextJavaFunctions maprDBSparkContext(JavaSparkContext sparkContext) {
        return new SparkContextJavaFunctions(JavaSparkContext.toSparkContext(sparkContext));
    }

    public static RDDJavaFunctions maprDBSparkContext(JavaRDD<OJAIDocument> rdd) {
        return new RDDJavaFunctions(rdd);
    }

    public static <D> RDDBeanJavaFunctions<D> maprDBSparkContext(JavaRDD<D> rdd, Class<D> clazz) {
        return new RDDBeanJavaFunctions<>(rdd, clazz);
    }

    public static <K> PairedRDDJavaFunctions<K> maprDBSparkContext(JavaPairRDD<K, OJAIDocument> rdd, Class<K> keyClazz) {
        return new PairedRDDJavaFunctions<>(rdd, keyClazz);
    }

    public static <K, V> PairedRDDBeanJavaFunctions<K, V> maprDBSparkContext(JavaPairRDD<K, V> rdd, Class<K> keyClazz, Class<V> valueClazz) {
        return new PairedRDDBeanJavaFunctions<>(rdd, keyClazz, valueClazz);
    }
}