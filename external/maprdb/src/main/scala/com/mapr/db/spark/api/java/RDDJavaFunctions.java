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

import com.mapr.db.spark.RDD.OJAIDocumentRDDFunctions;
import com.mapr.db.spark.impl.OJAIDocument;
import com.mapr.db.spark.writers.OJAIValue;
import com.mapr.db.spark.writers.OJAIValue$;
import org.apache.spark.api.java.JavaRDD;
import org.ojai.DocumentConstants;

public class RDDJavaFunctions {
    public final JavaRDD<OJAIDocument> rdd;
    public final OJAIDocumentRDDFunctions<OJAIDocument> ojaiDocumentRDDFunctions;


    public RDDJavaFunctions(JavaRDD<OJAIDocument> rdd) {
        OJAIValue<OJAIDocument> val = OJAIValue$.MODULE$.defaultOJAIDocument();
        this.rdd = rdd;
        this.ojaiDocumentRDDFunctions = new OJAIDocumentRDDFunctions<>(rdd.rdd(), val);
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