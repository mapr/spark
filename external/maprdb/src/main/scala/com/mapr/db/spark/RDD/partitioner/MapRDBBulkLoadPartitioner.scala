/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.RDD.partitioner

import java.nio.ByteBuffer

import com.mapr.db.impl.{ConditionImpl, IdCodec}
import com.mapr.db.spark.types.DBBinaryValue
import org.apache.spark.Partitioner
import org.ojai.Value
import scala.language.implicitConversions
import com.mapr.db.impl.ConditionNode.RowkeyRange
import com.mapr.db.spark.utils.{LoggingTrait, MapRDBUtils}
import com.mapr.db.spark.MapRDBSpark
import com.mapr.db.spark.dbclient.DBClient


object MapRDBPartitioner {
  def apply[T](table: String)(implicit k: OJAIKEY[T]): Partitioner = {
    var keys : Seq[Value] = DBClient().getTabletInfos(table).map(tableinfo =>
            IdCodec.decode(tableinfo.getCondition.asInstanceOf[ConditionImpl].getRowkeyRanges.get(0).getStopRow)).toSeq
    keys = keys.dropRight(1)
    if (keys.isEmpty)
      return MapRDBPartitioner[String](Seq.empty)

    if (!(k.getclass() == "String" && keys(0).getType == Value.Type.STRING) &&
        !((k.getclass() == "ByteBuffer" || k.getclass() == "DBBinaryValue") && keys(0).getType == Value.Type.BINARY))
      throw new RuntimeException("partition keys donot match: " + "RDD's key is of type " + k.getclass + " and table's ID is of type " + keys(0).getType)

    if (keys(0).getType == Value.Type.STRING) {
      return MapRDBPartitioner[String](keys.map(_.getString))
    } else {
      return MapRDBPartitioner(keys.map(value => MapRDBSpark.serializableBinaryValue(value.getBinary)))
    }
  }

  def apply[T](splits: Seq[T])(implicit ojaikey: OJAIKEY[T]): MapRDBSplitPartitioner[T] = {
    new MapRDBSplitPartitioner[T](splits) {}
  }
}

private[spark] abstract class MapRDBPartitioner extends Partitioner with LoggingTrait {
  @transient private[spark] val splits : Seq[Value]
}

//private[spark] abstract case class MapRDBTablePartitioner[T] (var tableName: String)(implicit ojaikey: OJAIKEY[T]) extends MapRDBPartitioner {
//
//  @transient private lazy val table =  DBClient().getTable(tableName)
//  @transient private lazy val tabletinfos = DBClient().getTabletInfos(tableName,null)
//  @transient private lazy val maptabletinfos: Map[ScanRange, Int] = tabletinfos.zipWithIndex.toMap
//
//  @transient override lazy val splits : Seq[Value] = {
//    val tbinfos = tabletinfos.map(tabletInfo => IdCodec.decode(tabletInfo.getCondition.asInstanceOf[ConditionImpl].getRowkeyRanges.get(0).getStopRow))
//    tbinfos.dropRight(1)
//  }
//
//  override def numPartitions: Int = tabletinfos.length
//
//  override def getPartition(key: Any): Int = {
//    maptabletinfos.get(ojaikey.getTabletInfo(maptabletinfos, ojaikey.getValue(key))) match {
//      case Some(a) => a
//      case None => logError("No Partition exists for key: "+ key)
//        throw new RuntimeException("no partition for this key")
//    }
//  }
//}

private[spark] abstract case class MapRDBSplitPartitioner[T] (@transient var inputSplits: Seq[T])(implicit ojaikey: OJAIKEY[T]) extends MapRDBPartitioner {

  private[spark] val splitsinBinary: Seq[DBBinaryValue] = inputSplits.map(ojaikey.getBytes(_)).map(ByteBuffer.wrap(_)).map(new DBBinaryValue(_))
  @transient private lazy val ranges: Seq[RowkeyRange] = ((Seq(null.asInstanceOf[ojaikey.Self]) ++ splitsinBinary.map(ojaikey.getValueFromBinary(_)))
                                                              .zip(splitsinBinary.map(ojaikey.getValueFromBinary(_)) ++ Seq(null.asInstanceOf[ojaikey.Self])))
                                                              .map(range => ojaikey.getRange(range))

  @transient override lazy val splits : Seq[Value] = {
    splitsinBinary.map(value => value.getByteBuffer().array()).map(bytes => IdCodec.decode(bytes))
  }

  override def numPartitions: Int = splitsinBinary.size + 1

  override def getPartition(key: Any): Int = {
    var partition: Int = 0;
    for(thisrange <- ranges) {
      if (MapRDBUtils.containsRow(ojaikey.getBytes(key), thisrange)) return partition
      else partition = partition+ 1
    }
    logError("No Partition exists for key: "+ key)
    throw new RuntimeException("no partition for this key")
  }
}
