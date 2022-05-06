/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.sql

import java.nio.ByteBuffer

import com.mapr.db.exceptions.TableExistsException
import com.mapr.db.impl.ConditionImpl
import com.mapr.db.spark.dbclient.DBClient
import com.mapr.db.spark.utils.MapRSpark
import org.ojai.DocumentConstants
import org.ojai.store.QueryCondition

import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StructField, StructType}

class DefaultSource
    extends DataSourceRegister
    with RelationProvider
    with SchemaRelationProvider
    with CreatableRelationProvider {

  override def shortName(): String = "maprdb"

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    val condition: Option[QueryCondition] = parameters
        .get("QueryCondition")
        .map(cond => ConditionImpl.parseFrom(ByteBuffer.wrap(cond.getBytes)))

    createMapRDBRelation(
      sqlContext,
      getTablePath(parameters),
      None,
      parameters.get("sampleSize"),
      parameters.getOrElse("bufferWrites", "true"),
      parameters.get("hintUsingIndex"),
      condition,
      parameters.get("ColumnProjection"),
      parameters.getOrElse("Operation", "InsertOrReplace"),
      parameters.getOrElse("FailOnConflict", "false"),
      parameters.filterKeys(k =>
        k.startsWith("ojai.mapr.query") || k.startsWith("spark.maprdb")).map(identity)
    )
  }

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String],
                              schema: StructType): BaseRelation = {
    val condition: Option[QueryCondition] = parameters
      .get("QueryCondition")
      .map(cond => ConditionImpl.parseFrom(ByteBuffer.wrap(cond.getBytes("ISO-8859-1"))))

    createMapRDBRelation(
      sqlContext,
      getTablePath(parameters),
      Some(schema),
      parameters.get("sampleSize"),
      parameters.getOrElse("bufferWrites", "true"),
      parameters.get("hintUsingIndex"),
      condition,
      parameters.get("ColumnProjection"),
      parameters.getOrElse("Operation", "InsertOrReplace"),
      parameters.getOrElse("FailOnConflict", "false"),
      parameters.filterKeys(k =>
        k.startsWith("ojai.mapr.query") || k.startsWith("spark.maprdb")).map(identity)
    )
  }

  override def createRelation(sqlContext: SQLContext,
                              mode: SaveMode,
                              parameters: Map[String, String],
                              data: DataFrame): BaseRelation = {

    val bufferWrites = parameters.getOrElse("bufferWrites", "true").toBoolean
    val tableName = getTablePath(parameters).getOrElse("")
    require(tableName.nonEmpty, "Table name must be defined")

    val idFieldPath = parameters.getOrElse("idFieldPath", DocumentConstants.ID_KEY)
    val condition: Option[QueryCondition] = parameters.get("QueryCondition")
      .map(cond => ConditionImpl.parseFrom(ByteBuffer.wrap(cond.getBytes)))

    lazy val tableExists = DBClient().tableExists(tableName)
    lazy val createTheTable = !tableExists
    lazy val bulkMode = parameters.getOrElse("bulkMode", "false").toBoolean

    val operation = parameters.getOrElse("Operation", "ErrorIfExists")
    mode match {
      case ErrorIfExists =>
      case _ => throw new UnsupportedOperationException(
          "Any mode operation is not supported for MapRDB Table." +
            "Please use Operation option instead")
    }

    operation match {
      case "Insert" =>
        MapRSpark.insert(data,
                         tableName,
                         idFieldPath,
                         createTable = createTheTable,
                         bulkInsert = bulkMode,
                         bufferWrites = bufferWrites)

      case "InsertOrReplace" =>
        MapRSpark.save(data,
                       tableName,
                       idFieldPath,
                       createTable = createTheTable,
                       bulkInsert = bulkMode,
                       bufferWrites = bufferWrites)

      case "Delete" =>
        MapRSpark.delete(data,
                         tableName,
                         idFieldPath,
                         bufferWrites = bufferWrites)

      case "ErrorIfExists" =>
        if (tableExists) {
          throw new TableExistsException(
            "Table: " + tableName + " already Exists")
        } else {
          MapRSpark.save(data,
            tableName,
            idFieldPath,
            createTable = true,
            bulkInsert = bulkMode,
            bufferWrites = bufferWrites)
        }
      case "Overwrite" =>
        DBClient().deleteTable(tableName)
        MapRSpark.save(data,
                       tableName,
                       idFieldPath,
                       createTable = true,
                       bulkInsert = bulkMode,
                       bufferWrites = bufferWrites)
      case _ =>
        throw new UnsupportedOperationException("Not supported operation")
    }

    createMapRDBRelation(
      sqlContext,
      Some(tableName),
      Some(data.schema),
      parameters.get("sampleSize"),
      parameters.getOrElse("bufferWrites", "true"),
      parameters.get("hintUsingIndex"),
      condition,
      parameters.get("ColumnProjection"),
      parameters.getOrElse("Operation", "InsertOrReplace"),
      parameters.getOrElse("FailOnConflict", "false"),
      parameters.filterKeys(k =>
        k.startsWith("ojai.mapr.query") || k.startsWith("spark.maprdb")).map(identity)
    )
  }

  private def createMapRDBRelation(sqlContext: SQLContext,
                                   tableName: Option[String],
                                   userSchema: Option[StructType],
                                   sampleSize: Option[String],
                                   bufferWrites: String,
                                   hintUsingIndex: Option[String],
                                   queryCondition: Option[QueryCondition],
                                   colProjection: Option[String],
                                   Operation: String,
                                   failOnConflict: String,
                                   queryOptions: Map[String, String]): BaseRelation = {

    require(tableName.isDefined)
    val columns = colProjection.map(colList => colList.split(",")
        .toSeq
        .filter(_.nonEmpty))

    val failureOnConflict = failOnConflict.toBoolean

    val rdd = MapRSpark.builder()
      .sparkSession(sqlContext.sparkSession)
      .configuration()
      .setTable(tableName.get)
      .setBufferWrites(bufferWrites.toBoolean)
      .setHintUsingIndex(hintUsingIndex)
      .setCond(queryCondition)
      .setColumnProjection(columns)
      .setQueryOptions(queryOptions)
      .build()
      .toRDD(null)

    val schema: StructType = makeSchemaNullable(userSchema match {
      case Some(s) => s
      case None =>
        GenerateSchema(
          rdd,
          sampleSize.map(_.toDouble).getOrElse(GenerateSchema.SAMPLE_SIZE),
          failureOnConflict)
    })

    MapRDBRelation(tableName.get, schema, rdd, Operation)(sqlContext)
  }

  private def makeSchemaNullable(schema: StructType): StructType = {
    StructType(schema.map(field => {
      StructField(field.name, field.dataType, nullable = true, field.metadata  )
    }))
  }

  private def getTablePath(parameters: Map[String, String]): Option[String] = {
    val tablePath = parameters.get("tablePath")
    if (tablePath.isDefined) tablePath else parameters.get("tableName")
  }
}
