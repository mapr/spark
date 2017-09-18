/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
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

    public static <D> RDDBeanJavaFunctions maprDBSparkContext(JavaRDD<D> rdd, Class<D> clazz) {
        return new RDDBeanJavaFunctions(rdd, clazz);
    }

    public static <K> PairedRDDJavaFunctions maprDBSparkContext(JavaPairRDD<K, OJAIDocument> rdd, Class<K> keyClazz) {
        return new PairedRDDJavaFunctions(rdd, keyClazz);
    }

    public static <K, V> PairedRDDBeanJavaFunctions maprDBSparkContext(JavaPairRDD<K, V> rdd, Class<K> keyClazz, Class<V> valueClazz) {
        return new PairedRDDBeanJavaFunctions(rdd, keyClazz, valueClazz);
    }
}