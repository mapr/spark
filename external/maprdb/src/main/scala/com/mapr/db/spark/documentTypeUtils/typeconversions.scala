/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.documentTypeUtils

import java.nio.ByteBuffer

import org.ojai.Value
import org.ojai.types._

// scalastyle:off
sealed trait convert[A, B] {
  def get(value: A): B
}

object conversions {

  // string to other datatypes conversions
  val string2intconversion = new convert[Value, Int] {
    def get(value: Value) : Int = value.getString.toInt
  }
  val string2shortconversion = new  convert[Value, Short] {
    def get(value: Value): Short = value.getString.toShort
  }
  val string2binaryconversion = new convert[Value, ByteBuffer] {
    def get(value: Value): ByteBuffer = throw new Exception("string cannot be converted to binary")
  }
  val string2booleanconversion = new convert[Value, Boolean] {
    def get(value: Value): Boolean = value.getString.toBoolean
  }
  val string2arrayconversion = null
  val string2mapconversion = null
  val string2byteconversion = new convert[Value, Byte] {
    def get(value: Value): Byte = value.getString.toByte
  }
  val string2dateconversion = new convert[Value, ODate]{
    def get(value: Value): ODate = ODate.parse(value.getString)
  }
  val string2decimalconversion = new convert[Value, BigDecimal] {
    def get(value: Value): BigDecimal = new java.math.BigDecimal(value.getString)
  }
  val string2doubleconversion = new convert[Value, Double] {
    def get(value: Value): Double = value.getString.toDouble
  }
  val string2floatconversion = new convert[Value, Float] {
    def get(value: Value): Float = value.getString.toFloat
  }
  val string2intervalconversion = null
  val string2Longconversion = new convert[Value, Long]{
    def get(value: Value): Long = value.getString.toLong
  }
  val string2Stringconversion = new convert[Value, String] {
    def get(value: Value): String = value.getString
  }
  val string2Timeconversion = new convert[Value,OTime] {
    def get(value: Value): OTime = OTime.parse(value.getString)
  }
  val string2Timestampconversion = new convert[Value,OTimestamp] {
    def get(value: Value): OTimestamp = OTimestamp.parse(value.getString)
  }

  //boolean to other data types conversions
  val boolean2intconversion = new convert[Value, Int] {
    def get(value: Value) : Int = if (value.getBoolean) {
      1
    } else {
      0
    }
  }
  val boolean2shortconversion = new  convert[Value, Short] {
    def get(value: Value): Short = if (value.getBoolean) {
      1
    } else {
      0
    }
  }
  val boolean2binaryconversion = null
  val boolean2booleanconversion = new convert[Value, Boolean] {
    def get(value: Value): Boolean = value.getBoolean
  }
  val boolean2arrayconversion = null
  val boolean2mapconversion = null
  val boolean2byteconversion = new convert[Value, Byte] {
    def get(value: Value): Byte = if (value.getBoolean) {
      1
    } else {
      0
    }
  }
  val boolean2dateconversion = null
  val boolean2decimalconversion = null
  val boolean2doubleconversion = null
  val boolean2floatconversion = null
  val boolean2intervalconversion = null
  val boolean2Longconversion = new convert[Value, Long]{
    def get(value: Value): Long = if (value.getBoolean) {
      1
    } else {
      0
    }
  }
  val boolean2Stringconversion = new convert[Value, String] {
    def get(value: Value): String = value.getBoolean.toString
  }
  val boolean2Timeconversion = null
  val boolean2Timestampconversion = null

  //short to other data types conversions
  val short2intconversion = new convert[Value, Int] {
    def get(value: Value) : Int = value.getShort.toInt
  }
  val short2shortconversion = new  convert[Value, Short] {
    def get(value: Value): Short = value.getShort
  }
  val short2binaryconversion = null
  val short2booleanconversion = null
  val short2arrayconversion = null
  val short2mapconversion = null
  val short2byteconversion = new convert[Value, Byte] {
    def get(value: Value): Byte = value.getShort.toByte
  }
  val short2dateconversion = new convert[Value, ODate]{
    def get(value: Value): ODate = ODate.fromDaysSinceEpoch(value.getShort)
  }
  val short2decimalconversion = new convert[Value, BigDecimal] {
    def get(value: Value): BigDecimal = BigDecimal.decimal(value.getShort.toFloat)
  }
  val short2doubleconversion = new convert[Value, Double] {
    def get(value: Value): Double = value.getShort.toDouble
  }
  val short2floatconversion = new convert[Value, Float] {
    def get(value: Value): Float = value.getShort.toFloat
  }
  val short2intervalconversion = new convert[Value, OInterval]{
    def get(value: Value): OInterval = new OInterval(value.getShort)
  }
  val short2Longconversion = new convert[Value, Long]{
    def get(value: Value): Long = value.getShort
  }
  val short2Stringconversion = new convert[Value, String] {
    def get(value: Value): String = value.getShort.toString
  }
  val short2Timeconversion = new convert[Value,OTime] {
    def get(value: Value): OTime = new OTime(value.getShort)
  }
  val short2Timestampconversion = new convert[Value,OTimestamp] {
    def get(value: Value): OTimestamp = new OTimestamp(value.getShort)
  }

  //int to other datatype conversions
  val int2intconversion = new convert[Value, Int] {
    def get(value: Value) : Int = value.getInt
  }
  val int2shortconversion = new  convert[Value, Short] {
    def get(value: Value): Short = value.getInt.toShort
  }
  val int2binaryconversion = null
  val int2booleanconversion = null
  val int2arrayconversion = null
  val int2mapconversion = null
  val int2byteconversion = new convert[Value, Byte] {
    def get(value: Value): Byte = value.getInt.toByte
  }
  val int2dateconversion = new convert[Value, ODate]{
    def get(value: Value): ODate = ODate.fromDaysSinceEpoch(value.getInt)
  }
  val int2decimalconversion = new convert[Value, BigDecimal] {
    def get(value: Value): BigDecimal =  BigDecimal(value.getInt)
  }
  val int2doubleconversion = new convert[Value, Double] {
    def get(value: Value): Double = value.getInt
  }
  val int2floatconversion = new convert[Value, Float] {
    def get(value: Value): Float = value.getInt
  }
  val int2intervalconversion = new convert[Value, OInterval]{
    def get(value: Value): OInterval = new OInterval(value.getInt)
  }
  val int2Longconversion = new convert[Value, Long]{
    def get(value: Value): Long = value.getInt
  }
  val int2Stringconversion = new convert[Value, String] {
    def get(value: Value): String = value.getInt.toString
  }
  val int2Timeconversion = new convert[Value,OTime] {
    def get(value: Value): OTime = OTime.fromMillisOfDay(value.getInt)
  }
  val int2Timestampconversion = new convert[Value,OTimestamp] {
    def get(value: Value): OTimestamp = new OTimestamp(value.getInt)
  }


  //array to other datatype conversions
  val array2intconversion = null
  val array2shortconversion = null
  val array2binaryconversion = null
  val array2booleanconversion = null
  val array2arrayconversion = null
  val array2mapconversion = null
  val array2byteconversion = null
  val array2dateconversion = null
  val array2decimalconversion = null
  val array2doubleconversion = null
  val array2floatconversion = null
  val array2intervalconversion = null
  val array2Longconversion = null
  val array2Stringconversion = new convert[Value, String] {
    def get(value: Value): String = value.getList.toString
  }
  val array2Timeconversion = null
  val array2Timestampconversion = null

  //map to other datatypes conversions
  val map2intconversion = null
  val map2shortconversion = null
  val map2binaryconversion = null
  val map2booleanconversion = null
  val map2arrayconversion = null
  val map2mapconversion = null
  val map2byteconversion = null
  val map2dateconversion = null
  val map2decimalconversion = null
  val map2doubleconversion = null
  val map2floatconversion = null
  val map2intervalconversion = null
  val map2Longconversion = null
  val map2Stringconversion = new convert[Value, String] {
    def get(value: Value): String = value.getMap.toString
  }
  val map2Timeconversion = null
  val map2Timestampconversion = null

  //binary to other datatypes conversions
  val binary2intconversion = null
  val binary2shortconversion = null
  val binary2binaryconversion = new convert[Value, ByteBuffer] {
    def get(value: Value): ByteBuffer = value.getBinary
  }
  val binary2booleanconversion = null
  val binary2arrayconversion = null
  val binary2mapconversion = null
  val binary2byteconversion = null
  val binary2dateconversion = null
  val binary2decimalconversion = null
  val binary2doubleconversion = null
  val binary2floatconversion = null
  val binary2intervalconversion = null
  val binary2Longconversion = null
  val binary2Stringconversion = null
  val binary2Timeconversion = null
  val binary2Timestampconversion = null

  //byte to other datatypes
  val byte2intconversion = new convert[Value, Int] {
    def get(value: Value) : Int = value.getByte
  }
  val byte2shortconversion = new  convert[Value, Short] {
    def get(value: Value): Short = value.getByte
  }
  val byte2binaryconversion = null
  val byte2booleanconversion = null
  val byte2arrayconversion = null
  val byte2mapconversion = null
  val byte2byteconversion = new convert[Value, Byte] {
    def get(value: Value): Byte = value.getByte
  }
  val byte2dateconversion = new convert[Value, ODate]{
    def get(value: Value): ODate = ODate.fromDaysSinceEpoch(value.getByte)
  }
  val byte2decimalconversion = new convert[Value, BigDecimal] {
    def get(value: Value): BigDecimal = BigDecimal(value.getByte)
  }
  val byte2doubleconversion = new convert[Value, Double] {
    def get(value: Value): Double = value.getByte
  }
  val byte2floatconversion = new convert[Value, Float] {
    def get(value: Value): Float = value.getByte
  }
  val byte2intervalconversion = new convert[Value, OInterval]{
    def get(value: Value): OInterval = new OInterval(value.getByte)
  }
  val byte2Longconversion = new convert[Value, Long]{
    def get(value: Value): Long = value.getByte
  }
  val byte2Stringconversion = new convert[Value, String] {
    def get(value: Value): String = value.getByte.toString
  }
  val byte2Timeconversion = new convert[Value,OTime] {
    def get(value: Value): OTime = OTime.fromMillisOfDay(value.getByte)
  }
  val byte2Timestampconversion = new convert[Value,OTimestamp] {
    def get(value: Value): OTimestamp = new OTimestamp(value.getByte)
  }


  //date to other datatype conversions
  val date2intconversion = new convert[Value, Int] {
    def get(value: Value) : Int = value.getDateAsInt
  }
  val date2shortconversion = null
  val date2binaryconversion = new convert[Value, ByteBuffer] {
    def get(value: Value): ByteBuffer = throw new Exception("cannot convert date to binary")
  }
  val date2booleanconversion = new convert[Value, Boolean] {
    def get(value: Value): Boolean = throw new Exception("cannot convert date to boolean")
  }
  val date2arrayconversion = null
  val date2mapconversion = null
  val date2byteconversion = new convert[Value, Byte] {
    def get(value: Value): Byte = throw new Exception("cannot convert date to byte")
  }
  val date2dateconversion = new convert[Value, ODate]{
    def get(value: Value): ODate = value.getDate
  }
  val date2decimalconversion = new convert[Value, BigDecimal] {
    def get(value: Value): BigDecimal = BigDecimal(value.getDateAsInt)
  }
  val date2doubleconversion = new convert[Value, Double] {
    def get(value: Value): Double = value.getDateAsInt
  }
  val date2floatconversion = new convert[Value, Float] {
    def get(value: Value): Float = value.getDateAsInt
  }
  val date2intervalconversion = new convert[Value, OInterval]{
    def get(value: Value): OInterval = throw new Exception("cannot convert date to interval")
  }
  val date2Longconversion = new convert[Value, Long]{
    def get(value: Value): Long = value.getDateAsInt
  }
  val date2Stringconversion = new convert[Value, String] {
    def get(value: Value): String = value.getDate.toDateStr
  }
  val date2Timeconversion = new convert[Value,OTime] {
    def get(value: Value): OTime = throw new Exception("cannot convert date to time")
  }
  val date2Timestampconversion = new convert[Value,OTimestamp] {
    def get(value: Value): OTimestamp = {
      val date: ODate = value.getDate
      new OTimestamp(date.getYear, date.getMonth, date.getDayOfMonth, 0, 0, 0, 0)
    }
  }

  //decimal to other datatype conversions
  val decimal2intconversion = new convert[Value, Int] {
    def get(value: Value) : Int = throw new Exception("cannot convert decimal to int")
  }
  val decimal2shortconversion = new  convert[Value, Short] {
    def get(value: Value): Short = throw new Exception("cannot convert decimal to short")
  }
  val decimal2binaryconversion = new convert[Value, ByteBuffer] {
    def get(value: Value): ByteBuffer = throw new Exception("cannot convert decimal to Binary")
  }
  val decimal2booleanconversion = new convert[Value, Boolean] {
    def get(value: Value): Boolean = throw new Exception("cannot convert decimal to boolean")
  }
  val decimal2arrayconversion = null
  val decimal2mapconversion = null
  val decimal2byteconversion = new convert[Value, Byte] {
    def get(value: Value): Byte = throw new Exception("cannot convert decimal to byte")
  }
  val decimal2dateconversion = new convert[Value, ODate]{
    def get(value: Value): ODate = throw new Exception("cannot convert decimal to byte")
  }
  val decimal2decimalconversion = new convert[Value, BigDecimal] {
    def get(value: Value): BigDecimal = value.getDecimal
  }
  val decimal2doubleconversion = new convert[Value, Double] {
    def get(value: Value): Double = throw new Exception("cannot convert decimal to double")
  }
  val decimal2floatconversion = new convert[Value, Float] {
    def get(value: Value): Float = throw new Exception("cannot convert decimal to float")
  }
  val decimal2intervalconversion = new convert[Value, OInterval]{
    def get(value: Value): OInterval = throw new Exception("cannot convert decimal to interval")
  }
  val decimal2Longconversion = new convert[Value, Long]{
    def get(value: Value): Long = throw new Exception("cannot convert decimal to long")
  }
  val decimal2Stringconversion = new convert[Value, String] {
    def get(value: Value): String = value.getDecimal.toEngineeringString
  }
  val decimal2Timeconversion = new convert[Value,OTime] {
    def get(value: Value): OTime = throw new Exception("cannot convert decimal to time")
  }
  val decimal2Timestampconversion = new convert[Value,OTimestamp] {
    def get(value: Value): OTimestamp = throw new Exception("cannot convert decimal to timeStamp")
  }

  //double to other datatypes conversions
  val double2intconversion = new convert[Value, Int] {
    def get(value: Value) : Int = value.getDouble.toInt
  }
  val double2shortconversion = new  convert[Value, Short] {
    def get(value: Value): Short = throw new Exception("cannot convert double to short")
  }
  val double2binaryconversion = new convert[Value, ByteBuffer] {
    def get(value: Value): ByteBuffer = throw new Exception("cannot convert double to binary")
  }
  val double2booleanconversion = new convert[Value, Boolean] {
    def get(value: Value): Boolean = value.getBoolean
  }
  val double2arrayconversion = null
  val double2mapconversion = null
  val double2byteconversion = new convert[Value, Byte] {
    def get(value: Value): Byte = throw new Exception("cannot convert double to byte")
  }
  val double2dateconversion = new convert[Value, ODate]{
    def get(value: Value): ODate = throw new Exception("cannot convert double to date")
  }
  val double2decimalconversion = new convert[Value, BigDecimal] {
    def get(value: Value): BigDecimal = BigDecimal(value.getDouble)
  }
  val double2doubleconversion = new convert[Value, Double] {
    def get(value: Value): Double = value.getDouble
  }
  val double2floatconversion = new convert[Value, Float] {
    def get(value: Value): Float = throw new Exception("cannot convert double to float")
  }
  val double2intervalconversion = new convert[Value, OInterval]{
    def get(value: Value): OInterval = throw new Exception("cannot convert double to interval")
  }
  val double2Longconversion = new convert[Value, Long]{
    def get(value: Value): Long = throw new Exception("cannot convert double to long")
  }
  val double2Stringconversion = new convert[Value, String] {
    def get(value: Value): String = value.getDouble.toString
  }
  val double2Timeconversion = new convert[Value,OTime] {
    def get(value: Value): OTime = throw new Exception("cannot convert double to Time")
  }
  val double2Timestampconversion = new convert[Value,OTimestamp] {
    def get(value: Value): OTimestamp = throw new Exception("cannot convert double to timestamp")
  }

  //float to other datatypes conversions
  val float2intconversion = null
  val float2shortconversion = null
  val float2binaryconversion = null
  val float2booleanconversion = null
  val float2arrayconversion = null
  val float2mapconversion = null
  val float2byteconversion = null
  val float2dateconversion = null
  val float2decimalconversion = new convert[Value, BigDecimal] {
    def get(value: Value): BigDecimal = BigDecimal.decimal(value.getFloat)
  }
  val float2doubleconversion = new convert[Value, Double] {
    def get(value: Value): Double = value.getFloat
  }
  val float2floatconversion = new convert[Value, Float] {
    def get(value: Value): Float = value.getFloat
  }
  val float2intervalconversion = new convert[Value, OInterval]{
    def get(value: Value): OInterval = throw new Exception("cannot convert float to interval")
  }
  val float2Longconversion = new convert[Value, Long]{
    def get(value: Value): Long = throw new Exception("cannot convert float to long")
  }
  val float2Stringconversion = new convert[Value, String] {
    def get(value: Value): String = value.getFloat.toString
  }
  val float2Timeconversion = new convert[Value,OTime] {
    def get(value: Value): OTime = throw new Exception("cannot convert float to time")
  }
  val float2Timestampconversion = new convert[Value,OTimestamp] {
    def get(value: Value): OTimestamp = throw new Exception("cannot convert float to timestamp")
  }

  //interval to other types conversions
  val interval2intconversion = new convert[Value, Int] {
    def get(value: Value) : Int = value.getIntervalAsLong.toInt
  }
  val interval2shortconversion = new  convert[Value, Short] {
    def get(value: Value): Short = value.getIntervalAsLong.toShort
  }
  val interval2binaryconversion = new convert[Value, ByteBuffer] {
    def get(value: Value): ByteBuffer = throw new Exception("cannot convert interval to binary")
  }
  val interval2booleanconversion = new convert[Value, Boolean] {
    def get(value: Value): Boolean = throw new Exception("cannot convert interval to boolean")
  }
  val interval2arrayconversion = null
  val interval2mapconversion = null
  val interval2byteconversion = new convert[Value, Byte] {
    def get(value: Value): Byte = value.getIntervalAsLong.toByte
  }
  val interval2dateconversion = new convert[Value, ODate]{
    def get(value: Value): ODate = throw new Exception("cannot convert interval to date")
  }
  val interval2decimalconversion = new convert[Value, BigDecimal] {
    def get(value: Value): BigDecimal = throw new Exception("cannot convert interval to decimal")
  }
  val interval2doubleconversion = new convert[Value, Double] {
    def get(value: Value): Double = throw new Exception("cannot convet interval to double")
  }
  val interval2floatconversion = new convert[Value, Float] {
    def get(value: Value): Float = throw new Exception("cannot convert interval to float")
  }
  val interval2intervalconversion = new convert[Value, OInterval]{
    def get(value: Value): OInterval = value.getInterval
  }
  val interval2Longconversion = new convert[Value, Long]{
    def get(value: Value): Long = value.getIntervalAsLong
  }
  val interval2Stringconversion = new convert[Value, String] {
    def get(value: Value): String = value.getInterval.toString
  }
  val interval2Timeconversion = new convert[Value,OTime] {
    def get(value: Value): OTime = throw new Exception("cannot convert interval to time")
  }
  val interval2Timestampconversion = new convert[Value,OTimestamp] {
    def get(value: Value): OTimestamp = throw new Exception("cannot conver interval to timestamp")
  }

  //long to other datatypes convesions
  val long2intconversion = new convert[Value, Int] {
    def get(value: Value) : Int = value.getLong.toInt
  }
  val long2shortconversion = new  convert[Value, Short] {
    def get(value: Value): Short = value.getLong.toShort
  }
  val long2binaryconversion = new convert[Value, ByteBuffer] {
    def get(value: Value): ByteBuffer = throw new Exception("cannot convert from long to binary")
  }
  val long2booleanconversion = new convert[Value, Boolean] {
    def get(value: Value): Boolean = value.getBoolean
  }
  val long2arrayconversion = null
  val long2mapconversion = null
  val long2byteconversion = new convert[Value, Byte] {
    def get(value: Value): Byte = value.getLong.toByte
  }
  val long2dateconversion = new convert[Value, ODate]{
    def get(value: Value): ODate = ODate.fromDaysSinceEpoch(value.getLong.toInt)
  }
  val long2decimalconversion = new convert[Value, BigDecimal] {
    def get(value: Value): BigDecimal = BigDecimal(value.getLong)
  }
  val long2doubleconversion = new convert[Value, Double] {
    def get(value: Value): Double = value.getLong
  }
  val long2floatconversion = new convert[Value, Float] {
    def get(value: Value): Float = value.getLong
  }
  val long2intervalconversion = new convert[Value, OInterval]{
    def get(value: Value): OInterval = new OInterval(value.getLong)
  }
  val long2Longconversion = new convert[Value, Long]{
    def get(value: Value): Long = value.getLong
  }
  val long2Stringconversion = new convert[Value, String] {
    def get(value: Value): String = value.getLong.toString
  }
  val long2Timeconversion = new convert[Value,OTime] {
    def get(value: Value): OTime = new OTime(value.getLong)
  }
  val long2Timestampconversion = new convert[Value,OTimestamp] {
    def get(value: Value): OTimestamp = new OTimestamp(value.getLong)
  }

  //time to other datatype conversions
  val time2intconversion = new convert[Value, Int] {
    def get(value: Value) : Int = value.getTimeAsInt
  }
  val time2shortconversion = new  convert[Value, Short] {
    def get(value: Value): Short = value.getTimeAsInt.toShort
  }
  val time2binaryconversion = new convert[Value, ByteBuffer] {
    def get(value: Value): ByteBuffer = throw new Exception("cannot convert time to binary")
  }
  val time2booleanconversion = new convert[Value, Boolean] {
    def get(value: Value): Boolean = throw new Exception("cannot convert time to boolean")
  }
  val time2arrayconversion = null
  val time2mapconversion = null
  val time2byteconversion = new convert[Value, Byte] {
    def get(value: Value): Byte = throw new Exception("cannot convert time to byte")
  }
  val time2dateconversion = new convert[Value, ODate]{
    def get(value: Value): ODate = throw new Exception("cannot convert time to date")
  }
  val time2decimalconversion = new convert[Value, BigDecimal] {
    def get(value: Value): BigDecimal = throw new Exception("cannot convert time to decimal")
  }
  val time2doubleconversion = new convert[Value, Double] {
    def get(value: Value): Double = throw new Exception("canot convert time to double")
  }
  val time2floatconversion = new convert[Value, Float] {
    def get(value: Value): Float = throw new Exception("cannot convert time to float")
  }
  val time2intervalconversion = new convert[Value, OInterval]{
    def get(value: Value): OInterval = throw new Exception("cannot convert time to interval")
  }
  val time2Longconversion = new convert[Value, Long]{
    def get(value: Value): Long = value.getTimeAsInt
  }
  val time2Stringconversion = new convert[Value, String] {
    def get(value: Value): String = value.getTime.toFullTimeStr
  }
  val time2Timeconversion = new convert[Value,OTime] {
    def get(value: Value): OTime = value.getTime
  }
  val time2Timestampconversion = new convert[Value,OTimestamp] {
    def get(value: Value): OTimestamp = throw new Exception("cannot conver time to timestamp")
  }

  //timestamp to other datatypes conversions
  val timestamp2intconversion = new convert[Value, Int] {
    def get(value: Value) : Int = value.getTimestampAsLong.toInt
  }
  val timestamp2shortconversion = new  convert[Value, Short] {
    def get(value: Value): Short = value.getTimestampAsLong.toShort
  }
  val timestamp2binaryconversion = new convert[Value, ByteBuffer] {
    def get(value: Value): ByteBuffer = throw new Exception("cannot convert timetstamp to binary")
  }
  val timestamp2booleanconversion = new convert[Value, Boolean] {
    def get(value: Value): Boolean = throw new Exception("cannot convert timestamp to boolean")
  }
  val timestamp2arrayconversion = null
  val timestamp2mapconversion = null
  val timestamp2byteconversion = new convert[Value, Byte] {
    def get(value: Value): Byte = throw new Exception("cannot convert timestamp to byte")
  }
  val timestamp2dateconversion = new convert[Value, ODate]{
    def get(value: Value): ODate = null
  }
  val timestamp2decimalconversion = new convert[Value, BigDecimal] {
    def get(value: Value): BigDecimal = null
  }
  val timestamp2doubleconversion = new convert[Value, Double] {
    def get(value: Value): Double = 0
  }
  val timestamp2floatconversion = new convert[Value, Float] {
    def get(value: Value): Float = 0
  }
  val timestamp2intervalconversion = new convert[Value, OInterval]{
    def get(value: Value): OInterval = null
  }
  val timestamp2Longconversion = new convert[Value, Long]{
    def get(value: Value): Long = 0
  }
  val timestamp2Stringconversion = new convert[Value, String] {
    def get(value: Value): String = null
  }
  val timestamp2Timeconversion = new convert[Value,OTime] {
    def get(value: Value): OTime = null
  }
  val timestamp2Timestampconversion = new convert[Value,OTimestamp] {
    def get(value: Value): OTimestamp = value.getTimestamp
  }
}

object typeconversions {
  def convert[B](value: Value, from: Value.Type, to: Value.Type): B = {
    val func = Option(typeconversions.conversionFunctions(from.getCode-1)(to.getCode-1))
    func match {
      case Some(a) => a.get(value).asInstanceOf[B]
      case None => throw new Exception("incompatible types")
    }
  }

  /*
   * This section deals with type casting of data whenever it is possible.
   * If a field in the table has different type for the data across different documents
   * Consider field "a.c" has integer type data but it is specified as int in some documents and string in some documents.
   * If the user specifies all the data to be represented as int then, convert the string data to int whenever it is possible
   * otherwise throw an exception.
   */
  val conversionFunctions = Array(Array(conversions.interval2Timeconversion,conversions.interval2Timeconversion,conversions.interval2Timeconversion,conversions.interval2Timeconversion,conversions.interval2Timeconversion,conversions.interval2Timeconversion,conversions.interval2Timeconversion,conversions.interval2Timeconversion,conversions.interval2Timeconversion,conversions.interval2Timeconversion,conversions.interval2Timeconversion,conversions.interval2Timeconversion,conversions.interval2Timeconversion,conversions.interval2Timeconversion,conversions.interval2Timeconversion,conversions.interval2Timeconversion,conversions.interval2Timeconversion,conversions.interval2Timeconversion),
    Array(conversions.interval2Timeconversion,conversions.boolean2booleanconversion ,conversions.boolean2Stringconversion ,conversions.boolean2byteconversion ,
      conversions.boolean2shortconversion, conversions.boolean2intconversion,conversions.boolean2Longconversion,conversions.boolean2floatconversion,conversions
        .boolean2doubleconversion,
      conversions.boolean2decimalconversion, conversions.boolean2dateconversion,conversions.boolean2Timeconversion,conversions.boolean2Timestampconversion,conversions
        .boolean2intervalconversion, conversions.boolean2binaryconversion, conversions.boolean2mapconversion,conversions.boolean2arrayconversion),
    Array(conversions.interval2Timeconversion,conversions.string2booleanconversion ,conversions.string2Stringconversion ,conversions.string2byteconversion ,
      conversions.string2shortconversion, conversions.string2intconversion,conversions.string2Longconversion,conversions.string2floatconversion,conversions.string2doubleconversion,
      conversions.string2decimalconversion, conversions.string2dateconversion,conversions.string2Timeconversion,conversions.string2Timestampconversion,conversions
        .string2intervalconversion, conversions.string2binaryconversion, conversions.string2mapconversion,conversions.string2arrayconversion),
    Array(conversions.interval2Timeconversion,conversions.byte2booleanconversion ,conversions.byte2Stringconversion ,conversions.byte2byteconversion ,
      conversions.byte2shortconversion, conversions.byte2intconversion,conversions.byte2Longconversion,conversions.byte2floatconversion,conversions.byte2doubleconversion,
      conversions.byte2decimalconversion, conversions.byte2dateconversion,conversions.byte2Timeconversion,conversions.byte2Timestampconversion,conversions
        .byte2intervalconversion, conversions.byte2binaryconversion, conversions.byte2mapconversion,conversions.byte2arrayconversion),
    Array(conversions.interval2Timeconversion,conversions.short2booleanconversion ,conversions.short2Stringconversion ,conversions.short2byteconversion ,
      conversions.short2shortconversion, conversions.short2intconversion,conversions.short2Longconversion,conversions.short2floatconversion,conversions.short2doubleconversion,
      conversions.short2decimalconversion, conversions.short2dateconversion,conversions.short2Timeconversion,conversions.short2Timestampconversion,conversions
        .short2intervalconversion, conversions.short2binaryconversion, conversions.short2mapconversion,conversions.short2arrayconversion),
    Array(conversions.interval2Timeconversion,conversions.int2booleanconversion ,conversions.int2Stringconversion ,conversions.int2byteconversion ,
      conversions.int2shortconversion, conversions.int2intconversion,conversions.int2Longconversion,conversions.int2floatconversion,conversions.int2doubleconversion,
      conversions.int2decimalconversion, conversions.int2dateconversion,conversions.int2Timeconversion,conversions.int2Timestampconversion,conversions
        .int2intervalconversion, conversions.int2binaryconversion, conversions.int2mapconversion,conversions.int2arrayconversion),
    Array(conversions.interval2Timeconversion,conversions.long2booleanconversion ,conversions.long2Stringconversion ,conversions.long2byteconversion ,
      conversions.long2shortconversion, conversions.long2intconversion,conversions.long2Longconversion,conversions.long2floatconversion,conversions.long2doubleconversion,
      conversions.long2decimalconversion, conversions.long2dateconversion,conversions.long2Timeconversion,conversions.long2Timestampconversion,conversions
        .long2intervalconversion, conversions.long2binaryconversion, conversions.long2mapconversion,conversions.long2arrayconversion),
    Array(conversions.interval2Timeconversion,conversions.float2booleanconversion ,conversions.float2Stringconversion ,conversions.float2byteconversion ,
      conversions.float2shortconversion, conversions.float2intconversion,conversions.float2Longconversion,conversions.float2floatconversion,conversions.float2doubleconversion,
      conversions.float2decimalconversion, conversions.float2dateconversion,conversions.float2Timeconversion,conversions.float2Timestampconversion,conversions
        .float2intervalconversion, conversions.float2binaryconversion, conversions.float2mapconversion,conversions.float2arrayconversion),
    Array(conversions.interval2Timeconversion,conversions.double2booleanconversion ,conversions.double2Stringconversion ,conversions.double2byteconversion ,
      conversions.double2shortconversion, conversions.double2intconversion,conversions.double2Longconversion,conversions.double2floatconversion,conversions.double2doubleconversion,
      conversions.double2decimalconversion, conversions.double2dateconversion,conversions.double2Timeconversion,conversions.double2Timestampconversion,conversions
        .double2intervalconversion, conversions.double2binaryconversion, conversions.double2mapconversion,conversions.double2arrayconversion),
    Array(conversions.interval2Timeconversion,conversions.decimal2booleanconversion ,conversions.decimal2Stringconversion ,conversions.decimal2byteconversion ,
      conversions.decimal2shortconversion, conversions.decimal2intconversion,conversions.decimal2Longconversion,conversions.decimal2floatconversion,conversions.decimal2doubleconversion,
      conversions.decimal2decimalconversion, conversions.decimal2dateconversion,conversions.decimal2Timeconversion,conversions.decimal2Timestampconversion,conversions
        .decimal2intervalconversion, conversions.decimal2binaryconversion, conversions.decimal2mapconversion,conversions.decimal2arrayconversion),
    Array(conversions.interval2Timeconversion,conversions.date2booleanconversion ,conversions.date2Stringconversion ,conversions.date2byteconversion ,
      conversions.date2shortconversion, conversions.date2intconversion,conversions.date2Longconversion,conversions.date2floatconversion,conversions.date2doubleconversion,
      conversions.date2decimalconversion, conversions.date2dateconversion,conversions.date2Timeconversion,conversions.date2Timestampconversion,conversions
        .date2intervalconversion, conversions.date2binaryconversion, conversions.date2mapconversion,conversions.date2arrayconversion),
    Array(conversions.interval2Timeconversion,conversions.time2booleanconversion ,conversions.time2Stringconversion ,conversions.time2byteconversion ,
      conversions.time2shortconversion, conversions.time2intconversion,conversions.time2Longconversion,conversions.time2floatconversion,conversions.time2doubleconversion,
      conversions.time2decimalconversion, conversions.time2dateconversion,conversions.time2Timeconversion,conversions.time2Timestampconversion,conversions
        .time2intervalconversion, conversions.time2binaryconversion, conversions.time2mapconversion,conversions.time2arrayconversion),
    Array(conversions.interval2Timeconversion,conversions.timestamp2booleanconversion ,conversions.timestamp2Stringconversion ,conversions.timestamp2byteconversion ,
      conversions.timestamp2shortconversion, conversions.timestamp2intconversion,conversions.timestamp2Longconversion,conversions.timestamp2floatconversion,conversions.timestamp2doubleconversion,
      conversions.timestamp2decimalconversion, conversions.timestamp2dateconversion,conversions.timestamp2Timeconversion,conversions.timestamp2Timestampconversion,conversions
        .timestamp2intervalconversion, conversions.timestamp2binaryconversion, conversions.timestamp2mapconversion,conversions.timestamp2arrayconversion),
    Array(conversions.interval2Timeconversion,conversions.interval2booleanconversion ,conversions.interval2Stringconversion ,conversions.interval2byteconversion ,
      conversions.interval2shortconversion, conversions.interval2intconversion,conversions.interval2Longconversion,conversions.interval2floatconversion,conversions.interval2doubleconversion,
      conversions.interval2decimalconversion, conversions.interval2dateconversion,conversions.interval2Timeconversion,conversions.interval2Timestampconversion,conversions
        .interval2intervalconversion, conversions.interval2binaryconversion, conversions.interval2mapconversion,conversions.interval2arrayconversion),
    Array(conversions.interval2Timeconversion,conversions.binary2booleanconversion ,conversions.binary2Stringconversion ,conversions.binary2byteconversion ,
      conversions.binary2shortconversion, conversions.binary2intconversion,conversions.binary2Longconversion,conversions.binary2floatconversion,conversions.binary2doubleconversion,
      conversions.binary2decimalconversion, conversions.binary2dateconversion,conversions.binary2Timeconversion,conversions.binary2Timestampconversion,conversions
        .binary2intervalconversion, conversions.binary2binaryconversion, conversions.binary2mapconversion,conversions.binary2arrayconversion),
    Array(conversions.interval2Timeconversion,conversions.map2booleanconversion ,conversions.map2Stringconversion ,conversions.map2byteconversion ,
      conversions.map2shortconversion, conversions.map2intconversion,conversions.map2Longconversion,conversions.map2floatconversion,conversions.map2doubleconversion,
      conversions.map2decimalconversion, conversions.map2dateconversion,conversions.map2Timeconversion,conversions.map2Timestampconversion,conversions
        .map2intervalconversion, conversions.map2binaryconversion, conversions.map2mapconversion,conversions.map2arrayconversion),
    Array(conversions.interval2Timeconversion,conversions.array2booleanconversion ,conversions.array2Stringconversion ,conversions.array2byteconversion ,
      conversions.array2shortconversion, conversions.array2intconversion,conversions.array2Longconversion,conversions.array2floatconversion,conversions.array2doubleconversion,
      conversions.array2decimalconversion, conversions.array2dateconversion,conversions.array2Timeconversion,conversions.array2Timestampconversion,conversions
        .array2intervalconversion, conversions.array2binaryconversion, conversions.array2mapconversion,conversions.array2arrayconversion)
  )}
// scalastyle:on
