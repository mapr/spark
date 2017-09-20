/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.api.java;

import com.mapr.db.spark.RDD.PairedDocumentRDDFunctions;
import com.mapr.db.spark.utils.MapRDBUtils;
import com.mapr.db.spark.writers.OJAIKey;
import com.mapr.db.spark.writers.OJAIValue;
import com.mapr.db.spark.writers.OJAIValue$;
import org.apache.spark.api.java.JavaPairRDD;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

public class PairedRDDBeanJavaFunctions<K, V> {
    public final JavaPairRDD<K,V> rdd;
    public final PairedDocumentRDDFunctions<K, V> ojaiDocumentRDDFunctions;


    public PairedRDDBeanJavaFunctions(JavaPairRDD<K, V> rdd, Class<K> keyClazz, Class<V> valueClazz) {
        OJAIValue<V> val = OJAIValue$.MODULE$.overrideDefault();
        ClassTag<K> ct = ClassTag$.MODULE$.apply(keyClazz);
        OJAIKey<K> key = MapRDBUtils.getOjaiKey(ct);
        this.rdd = rdd;
        this.ojaiDocumentRDDFunctions = new PairedDocumentRDDFunctions<>(rdd.rdd(), key, val);
    }

    public void saveToMapRDB(String tableName) {
        this.saveToMapRDB(tableName, false, false);
    }

    public void saveToMapRDB(String tableName, boolean createTable) {
        this.saveToMapRDB(tableName, createTable, false);
    }

    public void saveToMapRDB(String tableName, boolean createTable, boolean bulkInsert) {
        this.ojaiDocumentRDDFunctions.saveToMapRDB(tableName, createTable, bulkInsert);
    }
}