/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.sql

import java.nio.ByteBuffer

import com.mapr.db.exceptions.TableExistsException
import com.mapr.db.impl.ConditionImpl
import com.mapr.db.spark.dbclient.DBClient
import com.mapr.db.spark.utils.MapRSpark
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SaveMode._
import org.ojai.store.QueryCondition

class DefaultSource
    extends DataSourceRegister
    with RelationProvider
    with SchemaRelationProvider
    with CreatableRelationProvider {

  override def shortName(): String = "maprdb"

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    val condition: Option[QueryCondition] =
      parameters
        .get("QueryCondition")
        .map(cond => ConditionImpl.parseFrom(ByteBuffer.wrap(cond.getBytes)))
    createMapRDBRelation(
      sqlContext,
      parameters.get("tableName"),
      None,
      parameters.get("sampleSize"),
      condition,
      parameters.get("ColumnProjection"),
      parameters.getOrElse("Operation", "InsertOrReplace"),
      parameters.getOrElse("FailOnConflict", "false")
    )
  }

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String],
                              schema: StructType): BaseRelation = {
    val condition: Option[QueryCondition] = parameters
      .get("QueryCondition")
      .map(cond => ConditionImpl.parseFrom(ByteBuffer.wrap(cond.getBytes)))
    createMapRDBRelation(
      sqlContext,
      parameters.get("tableName"),
      Some(schema),
      parameters.get("sampleSize"),
      condition,
      parameters.get("ColumnProjection"),
      parameters.getOrElse("Operation", "InsertOrReplace"),
      parameters.getOrElse("FailOnConflict", "false")
    )
  }

  override def createRelation(sqlContext: SQLContext,
                              mode: SaveMode,
                              parameters: Map[String, String],
                              data: DataFrame): BaseRelation = {


    require(parameters.get("tableName").isDefined, "Table name must be defined")
    val idFieldPath = parameters.getOrElse("idFieldPath", "_id")
    val condition: Option[QueryCondition] = parameters
      .get("QueryCondition")
      .map(cond => ConditionImpl.parseFrom(ByteBuffer.wrap(cond.getBytes)))
    lazy val tableExists =
      DBClient().tableExists(parameters("tableName"))
    lazy val tableName = parameters("tableName")
    lazy val createTheTable = !tableExists
    lazy val bulkMode = parameters.getOrElse("bulkMode", "false").toBoolean
    val operation = parameters.getOrElse("Operation", "ErrorIfExists")
    mode match {
      case ErrorIfExists =>
      case _ =>
        throw new UnsupportedOperationException(
          "Any mode operation is not supported for MapRDB Table." +
            "Please use Operation option instead")
    }

    operation match {
      case "Insert" =>
        MapRSpark.insert(data,
                         tableName,
                         idFieldPath,
                         createTable = createTheTable,
                         bulkInsert = bulkMode)

      case "InsertOrReplace" =>
        MapRSpark.save(data,
                       tableName,
                       idFieldPath,
                       createTable = createTheTable,
                       bulkInsert = bulkMode)


      case "ErrorIfExists" =>
        if (tableExists) {
          throw new TableExistsException(
            "Table: " + tableName + " already Exists")
        } else {
          MapRSpark.save(data,
            tableName,
            idFieldPath,
            createTable = true,
            bulkInsert = bulkMode)
        }
      case "Overwrite" =>
        DBClient().deleteTable(tableName)
        MapRSpark.save(data,
                       tableName,
                       idFieldPath,
                       createTable = true,
                       bulkInsert = bulkMode)
      case "Update" =>
        MapRSpark.update(data,
                         tableName,
                         idFieldPath,
                         createTable = false,
                         bulkInsert = bulkMode)
      case _ =>
        throw new UnsupportedOperationException("Not supported operation")
    }

    createMapRDBRelation(
      sqlContext,
      Some(tableName),
      Some(data.schema),
      parameters.get("sampleSize"),
      condition,
      parameters.get("ColumnProjection"),
      parameters.getOrElse("Operation", "InsertOrReplace"),
      parameters.getOrElse("FailOnConflict", "false")
    )

  }

  private def createMapRDBRelation(sqlContext: SQLContext,
                                   tableName: Option[String],
                                   userSchema: Option[StructType],
                                   sampleSize: Option[String],
                                   queryCondition: Option[QueryCondition],
                                   colProjection: Option[String],
                                   Operation: String,
                                   failOnConflict: String): BaseRelation = {

    require(tableName.isDefined)
    val columns =
      colProjection.map(colList => colList.split(",").toSeq.filter(_.nonEmpty))
    val failureOnConflict = failOnConflict.toBoolean

    val rdd = MapRSpark.builder
      .sparkContext(sqlContext.sparkContext)
      .sparkSession(sqlContext.sparkSession)
      .configuration()
      .setTable(tableName.get)
      .setCond(queryCondition)
      .setColumnProjection(columns)
      .build
      .toRDD(null)

    val schema: StructType = userSchema match {
      case Some(s) => s
      case None =>
        GenerateSchema(
          rdd,
          sampleSize.map(_.toDouble).getOrElse(GenerateSchema.SAMPLE_SIZE),
          failureOnConflict)
    }

    MapRDBRelation(tableName.get, schema, rdd, Operation)(sqlContext)
  }
}
