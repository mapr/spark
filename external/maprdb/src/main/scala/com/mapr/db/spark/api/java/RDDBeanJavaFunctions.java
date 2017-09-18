/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.api.java;

import com.mapr.db.spark.RDD.OJAIDocumentRDDFunctions;
import com.mapr.db.spark.writers.OJAIValue;
import com.mapr.db.spark.writers.OJAIValue$;
import org.apache.spark.api.java.JavaRDD;
import org.ojai.DocumentConstants;

public class RDDBeanJavaFunctions<D> {
    public final JavaRDD<D> rdd;
    public final OJAIDocumentRDDFunctions<D> ojaiDocumentRDDFunctions;


    public RDDBeanJavaFunctions(JavaRDD<D> rdd, Class<D> clazz) {
        OJAIValue<D> val = OJAIValue$.MODULE$.overrideJavaDefault();
        this.rdd = rdd;
        this.ojaiDocumentRDDFunctions = new OJAIDocumentRDDFunctions(rdd.rdd(), val);
    }

    public void saveToMapRDB(String tableName) {
        this.saveToMapRDB(tableName, false, false, DocumentConstants.ID_KEY);
    }

    public void saveToMapRDB(String tableName, boolean createTable) {
        this.saveToMapRDB(tableName, createTable, false, DocumentConstants.ID_KEY);
    }

    public void saveToMapRDB(String tableName, boolean createTable, boolean bulkInsert) {
        this.saveToMapRDB(tableName, createTable, bulkInsert, DocumentConstants.ID_KEY);
    }

    public void saveToMapRDB(String tableName, boolean createTable, boolean bulkInsert, String ID) {
        this.ojaiDocumentRDDFunctions.saveToMapRDB(tableName, createTable, bulkInsert, ID);
    }
}