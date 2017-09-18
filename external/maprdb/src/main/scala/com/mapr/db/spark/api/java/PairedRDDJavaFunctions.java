/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.api.java;

import com.mapr.db.spark.RDD.PairedDocumentRDDFunctions;
import com.mapr.db.spark.impl.OJAIDocument;
import com.mapr.db.spark.utils.MapRDBUtils;
import com.mapr.db.spark.writers.OJAIKey;
import com.mapr.db.spark.writers.OJAIValue;
import com.mapr.db.spark.writers.OJAIValue$;
import org.apache.spark.api.java.JavaPairRDD;
import scala.reflect.ClassTag$;

public class PairedRDDJavaFunctions<K> {
    public final JavaPairRDD<K,OJAIDocument> rdd;
    public final PairedDocumentRDDFunctions<K, OJAIDocument> ojaiDocumentRDDFunctions;


    public PairedRDDJavaFunctions(JavaPairRDD<K, OJAIDocument> rdd, Class<K> keyClazz) {
        OJAIValue<OJAIDocument> val = OJAIValue$.MODULE$.defaultOJAIDocument();
        OJAIKey<K> key = MapRDBUtils.getOjaiKey(ClassTag$.MODULE$.apply(keyClazz));
        this.rdd = rdd;
        this.ojaiDocumentRDDFunctions = new PairedDocumentRDDFunctions(rdd.rdd(), key, val);
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