/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.api.java;

import com.mapr.db.spark.RDD.MapRDBBaseRDD;
import com.mapr.db.spark.RDD.RDDTYPE;
import com.mapr.db.spark.RDD.RDDTYPE$;
import com.mapr.db.spark.RDD.api.java.MapRDBJavaRDD;
import com.mapr.db.spark.impl.OJAIDocument;
import com.mapr.db.spark.utils.MapRSpark;
import org.apache.spark.SparkContext;
import scala.reflect.ClassTag$;
import org.apache.hadoop.conf.Configuration;


public class SparkContextJavaFunctions {
    public final SparkContext sparkContext;

    public SparkContextJavaFunctions(SparkContext sparkContext) {
        this.sparkContext = sparkContext;
    }

    public MapRDBJavaRDD<OJAIDocument> loadFromMapRDB(String tableName) {
        RDDTYPE<OJAIDocument> f = RDDTYPE$.MODULE$.defaultType();
        MapRDBBaseRDD<OJAIDocument> rdd = MapRSpark.builder().sparkContext(sparkContext)
                .configuration(new Configuration())
                .setTable(tableName).build().toJavaRDD(OJAIDocument.class, ClassTag$.MODULE$.apply(OJAIDocument.class), f);
        return new MapRDBJavaRDD<>(rdd, ClassTag$.MODULE$.apply(OJAIDocument.class));
    }

    public <D extends Object> MapRDBJavaRDD<D> loadFromMapRDB(String tableName, Class<D> clazz) {
        RDDTYPE<D> f = RDDTYPE$.MODULE$.overrideJavaDefaultType();
        MapRDBBaseRDD<D> rdd = MapRSpark.builder().sparkContext(sparkContext)
                .configuration(new Configuration())
                .setTable(tableName).build().toJavaRDD(clazz, ClassTag$.MODULE$.apply(clazz), f);
        return new MapRDBJavaRDD<D>(rdd, ClassTag$.MODULE$.apply(clazz));
    }
}
