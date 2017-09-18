package com.mapr.db.tests

import org.scalatest._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.mapr.db.testCases._

import scala.language.implicitConversions
import org.junit.experimental.categories.{Categories, Category}


@RunWith(classOf[JUnitRunner])
class AllTestsWithKryo extends FlatSpec with InitClusterEnableKryoSerialization with GivenWhenThen with Matchers {

  "Empty set" should "be counted" in {
    assert (com.mapr.db.testCases.MapRDBSparkTests.testEmptySet(sc,tableName))
  }

  "Shakespeare most famous quote" should "be counted" in {
    assert( com.mapr.db.testCases.MapRDBSparkTests.testShakespearesFamousQuote(sc, tableName))
  }

  "bean class parsing" should "be tested" in  {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testBeanClassParsing(sc, tableName))
  }

  "integer collection of zip codes" should "be tested " in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testIntegerZipCodes(sc, tableName))
  }

  "maprdb bean class" should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testingMapRDBBeanClass(sc, tableName))
  }

  "maprdb bean collect class" should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testMaprDBBeanClassWithCollect(sc, tableName))
  }

  "groupby key on ODate " should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testGroupByOnODate(sc, tableName))
  }

  "loading of ojai documents into rdd" should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testingLoadingOfOJAIDocuments(sc, tableName))
  }

  "cogroup test on ojai document rdd" should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testingCoGroupWithOJAIDOCUMENT(sc, tableName))
  }

  "cogroup on array value test on ojai document rdd" should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testingArrayValueWithOJAIDocument(sc, tableName))
  }

  "cogroup on array key test on ojai document rdd" should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testingCoGroupArrayKey(sc, tableName))
  }

  "cogroup on map value test on ojai document rdd" should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testingCoGroupOnMapValue(sc, tableName))
  }

  "cogroup on map key test on ojai document rdd" should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testingCoGroupOnMapKey(sc, tableName))
  }

  "cogroup on map key test on ojai document rdd with filter" should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testingCoGroupWithMapKeyWithFilter(sc, tableName))
  }

  "assigning a zipcode to ojai document" should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testingAssignmentOfZipCodeToOJAIDocument(sc, tableName))
  }

  "accessing fields of ojai document without passing parametric types" should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testingAccessOfFieldsOfOJAIDocumentWithParametricTypes(sc, tableName))
  }

  "accessing fields by passing parametric type" should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testingAccessOfFieldsOfOJAIDocumentWithParametricTypes2(sc, tableName))
  }

  "accessing only projected fields" should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testingAccessOfProjectedFields(sc, tableName))
  }

  "accessing only projected field paths" should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testingAccessOfProjectedFieldPaths(sc, tableName))
  }

  "saving the processed OJAIDocuments to a maprdb table" should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testingOfSavingTheProcessedOJAIDocuments(sc, tableName))
  }

  "map as a value" should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testingMapAsaValue(sc, tableName))
  }

  "map as a key" should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testingMapAsaKey(sc, tableName))
  }

  "array as value" should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testingArrayAsaValue(sc, tableName))
  }

  "array as key" should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testingArrayAsaKey(sc, tableName))
  }

  "ojai document parsing functionality" should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testingOJAIDocumentParsingFunctionality(sc, tableName))
  }

  "multiple data types in ojai document parsing functionality" should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testingMultipleDataTypesInOJAIDocument(sc, tableName))
  }

//  "multiple data types in ojai document parsing functionality and type casting" should "be tested" in {
//    assert(com.mapr.db.testCases.MapRDBSparkTests.testingMultipleDataTypesInOJAIDocumentAndTypeCasting(sc, tableName))
//  }

  "single data types (double) in ojai document parsing functionality and type casting" should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testingSingleDataTypeDoubleInOJAIAndTypeCasting(sc, tableName))
  }

  "tuple output of an RDD" should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testingTupleOutputOfAnRDD(sc, tableName))
  }

  "adding country into the address field of ojaidocument" should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testingAddingCountryInAddressField(sc, tableName))
  }

  "flat with map object" should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testCaseWithFlatMap(sc, tableName))
  }

  "testing binary type in ojaidocument" should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testingBinaryDataTypeInOJAIDocument(sc, tableName))
  }

  "testing map type in ojaidocument" should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testingMapTypeInOJAIDocument(sc, tableName))
  }

  "testing date type in ojaidocument" should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testingDateTypeInOJAIDocument(sc, tableName))
  }

  "testing save with any object" should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testingSaveWithAnyObject(sc, tableName))
  }


  "testing filter funciton on Map object" should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testingFilterFunctionOnMapObject(sc, tableName))
  }


  "testing filter funciton on array object" should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testingFilterFunctionOnArrayObject(sc, tableName))
  }

  "testing filter function on array object functional way" should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testingFilterFunctionOnArrayObjectFunctionalway(sc, tableName))
  }

  "test where class on loadMaPRDBTable" should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testingWhereClauseOnloadFromMapRDB(sc, tableName))
  }

  "partiton on MapRDBTable" should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testingPartitionOnloadFromMapRDB(sc, tableName))
  }

  "assignment of document as a value" should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testingAssignmentOfDocument(sc, tableName))
  }

  "testingJoinWithRDD" should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testingJoinWithRDD(sc, tableName))
  }

  "testingJoinWithRDDBean" should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testingJoinWithRDDBean(sc, tableName))
  }

  "testingBulkJoinWithRDD" should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testingBulkJoinWithRDD(sc, tableName))
  }

  "testingJoinWithOjaiRDDBean" should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testingJoinWithOjaiRDDBean(sc, tableName))
  }

  "testingUpdateMapRDBTable" should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testingUpdateMapRDBTable(sc, tableName))
  }

  "testingUpdateMapRDBTablePairedRDD" should "be tested" in {
    assert(com.mapr.db.testCases.MapRDBSparkTests.testingUpdateMapRDBTablePairedRDD(sc, tableName))
  }

  "testingCheckAndUpdateMapRDBTable" should "be tested" in {
    assert(MapRDBSparkTests.testingCheckAndUpdateMapRDBTable(sc, tableName))
  }

  "getter functionality for int" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testGetterFuncForInt(sc))
  }

  "getter functionality for byte" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testGetterFuncForByte(sc))
  }

  "getter functionality for string" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testGetterFuncForString(sc))
  }
  "getter functionality for short" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testGetterFuncForShort(sc))
  }
  "getter functionality for long" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testGetterFuncForLong(sc))
  }
  "getter functionality for float" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testGetterFuncForFloat(sc))
  }
  "getter functionality for double" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testGetterFuncForDouble(sc))
  }
  "getter functionality for time" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testGetterFuncForTime(sc))
  }
  "getter functionality for date" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testGetterFuncForDate(sc))
  }
  "getter functionality for timestamp" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testGetterFuncForTimeStamp(sc))
  }
  "getter functionality for binary" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testGetterFuncForBinary(sc))
  }
  "getter functionality for list" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testGetterFuncForList(sc))
  }
  "getter functionality for map" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testGetterFuncForMap(sc))
  }
  "getter functionality for int explicit" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testGetterFuncForIntExpl(sc))
  }
  "getter functionality for byte explicit" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testGetterFuncForByteExpl(sc))
  }
  "getter functionality for string explicit" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testGetterFuncForStringExpl(sc))
  }
  "getter functionality for short explicit" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testGetterFuncForShortExpl(sc))
  }
  "getter functionality for long explicit" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testGetterFuncForLongExpl(sc))
  }
  "getter functionality for float explicit" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testGetterFuncForFloatExpl(sc))
  }
  "getter functionality for double explicit" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testGetterFuncForDoubleExpl(sc))
  }
  "getter functionality for time explicit" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testGetterFuncForTimeExpl(sc))
  }
  "getter functionality for date explicit" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testGetterFuncForDateExpl(sc))
  }
  "getter functionality for timestamp explicit" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testGetterFuncForTimeStampExpl(sc))
  }
  "getter functionality for binary explicit" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testGetterFuncForBinaryExpl(sc))
  }
  "getter functionality for array explicit" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testGetterFuncForArrayExpl(sc))
  }
  "getter functionality for map explicit" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testGetterFuncForMapExpl(sc))
  }
  "setter functionality for int" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testSetterFuncForInt(sc))
  }
  "setter functionality for byte" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testSetterFuncForByte(sc))
  }
  "setter functionality for string" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testSetterFuncForString(sc))
  }
  "setter functionality for short" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testSetterFuncForShort(sc))
  }
  "setter functionality for long" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testSetterFuncForLong(sc))
  }
  "setter functionality for float" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testSetterFuncForFloat(sc))
  }
  "setter functionality for double" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testSetterFuncForDouble(sc))
  }
  "setter functionality for time" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testSetterFuncForTime(sc))
  }
  "setter functionality for date" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testSetterFuncForDate(sc))
  }
  "setter functionality for timestamp" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testSetterFuncForTimeStamp(sc))
  }
  "setter functionality for binary" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testSetterFuncForBinary(sc))
  }
  "setter functionality for binary with byteArray" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testSetterFuncForBinaryWithByteArr(sc))
  }
  "setter functionality for list " should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testSetterFuncForList(sc))
  }
  "setter functionality for map" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testSetterFuncForMap(sc))
  }
  "getter no data" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testGetterNoDataCase(sc))
  }
  "setter functionality for null data" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testSetterNullToDoc(sc))
  }
  "setter functionality for map string int" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testSetterFuncForMapStringInt(sc))
  }

  "Non dynamic setter functionality for int" should "be tested" in {
    assert(com.mapr.db.testCases.OjaiDocumentAccessTesting.testNonDynamicSetterFuncForInt(sc))
  }

  "loading with just table name" should "be tested" in {
    assert(com.mapr.db.testCases.LoadAndSaveTests.testingSimpleLoadTable(sc,tableName))
  }

  "loading with table name and specific columns" should "be tested" in {
    assert(com.mapr.db.testCases.LoadAndSaveTests.testingLoadTableWithSpecificColumns(sc, tableName))
  }

  "loading with table name and where condition" should "be tested" in {
    assert(com.mapr.db.testCases.LoadAndSaveTests.testingLoadTableWithWhereEQCondition(sc, tableName))
  }

  "loading with table name, where condition and select clause" should "be tested" in {
    assert(com.mapr.db.testCases.LoadAndSaveTests.testingLoadTableWithWhereEQAndSelectClause(sc, tableName))
  }

  "testingLoadTableWithWhereEQConditionAndSave" should "be tested" in {
    assert(com.mapr.db.testCases.LoadAndSaveTests.testingLoadTableWithWhereEQConditionAndSave(sc, tableName))
  }

  "saving a table" should "be tested" in {
    assert(com.mapr.db.testCases.LoadAndSaveTests.testingSimpleSaveTable(sc, tableName, tableName2))
  }

  "saving a table with different ID" should "be tested" in  {
    assert(com.mapr.db.testCases.LoadAndSaveTests.testingIDwithSaveToMapRDB(sc, tableName, tableName2))
  }

  "saving a table using bulk save mode" should "be tested" in {
    assert(com.mapr.db.testCases.LoadAndSaveTests.testingBulkSaveMode(sc, tableName, tableName2))
  }

  "saving a table using bulk save mode and bulkmode set to false for table" should "be tested" in {
    assert(com.mapr.db.testCases.LoadAndSaveTests.testingBulkSaveWithoutBulkModeSetInTable(sc, tableName, tableName2+"new1"))
  }

  "saving a table using bulk save mode with bean class" should "be tested" in {
    assert(com.mapr.db.testCases.LoadAndSaveTests.testingBulkSaveModeBeanClass(sc, tableName, tableName2))
  }

  "split partitioner with string types" should "be tested" in {
    assert(com.mapr.db.testCases.LoadAndSaveTests.testingSplitPartitioner(sc, tableName, tableName2+"new2"))
  }

  "split partitioner with binary types" should "be tested" in {
    assert(com.mapr.db.testCases.LoadAndSaveTests.testingSplitPartitionerWithBinaryData(sc, tableName, tableName2+"new"))
  }

  "split partitioner with bytebuffer types" should "be tested" in {
    assert(com.mapr.db.testCases.LoadAndSaveTests.testingSplitPartitionerWithByteBufferData(sc, tableName, tableName2 + "new"))
  }

  //Following are the condition testing
  "simple _id only condition" should "be tested" in {
    assert(PredicateTests.testingIdOnlyCondition(sc, PredicateTests.tableName));
  }

  "testingSimpleGTCondition" should "be tested" in {
    assert(PredicateTests.testingSimpleGTCondition(sc, PredicateTests.tableName))
  }
  "testingNotExistsCondition" should "be tested" in {
    assert(PredicateTests.testingNotExistsCondition(sc, PredicateTests.tableName))
  }

  "testingSimpleINCondition" should "be tested" in {
    assert(PredicateTests.testingSimpleINCondition(sc, PredicateTests.tableName))
  }

  "testingOTimeINCondition" should "be tested" in {
    assert(PredicateTests.testingOTimeINCondition(sc, PredicateTests.tableName))
  }

  "testingSimpleOTimeCondition" should "be tested" in {
    assert(PredicateTests.testingSimpleOTime(sc, PredicateTests.tableName))
  }

  "testingTYPEOFCondition" should "be tested" in {
    assert(PredicateTests.testingTYPEOFCondition(sc, PredicateTests.tableName))
  }

  "testingComplexAND_INcondition" should "be tested" in {
    assert(PredicateTests.testingComplexAND_INcondition(sc, PredicateTests.tableName))
  }

  "testingCompositeCondition" should "be tested" in {
    assert(PredicateTests.testingCompositeCondition(sc, PredicateTests.tableName))
  }

  "testingThreeConditions" should "be tested" in {
    assert(PredicateTests.testingThreeConditions(sc, PredicateTests.tableName))
  }

  "testingORCondition" should "be tested" in {
    assert(PredicateTests.testingORCondition(sc, PredicateTests.tableName))
  }

  "testingComplexConditonWithDate" should "be tested" in {
    assert(PredicateTests.testingComplexConditonWithDate(sc, PredicateTests.tableName))
  }

  "testingBetweenCondition" should "be tested" in {
    assert(PredicateTests.testingBetweenCondition(sc, PredicateTests.tableName))
  }

  "testingEqualityConditionOnSeq" should "be tested" in {
    assert(PredicateTests.testingEqualityConditionOnSeq(sc, PredicateTests.tableName))
  }

  "testingEqualtiyOnMapOfStrings" should "be tested" in {
    assert(PredicateTests.testingEqualtiyOnMapOfStrings(sc, PredicateTests.tableName))
  }

  "testingEqualityOnMapStringInteger" should "be tested" in {
    assert(PredicateTests.testingEqualityOnMapStringInteger(sc, PredicateTests.tableName))
  }

  "testingLikeCondition" should "be tested" in {
    assert(PredicateTests.testingLikeCondition(sc, PredicateTests.tableName))
  }

  "testingMatchesCondition" should "be tested" in {
    assert(PredicateTests.testingMatchesCondition(sc, PredicateTests.tableName))
  }

  "testing not equality on id" should "be tested" in {
    assert(PredicateTests.testingNotEqualityOnID(sc, PredicateTests.tableName))
  }

  "testingNotEqualityOnMapStringInteger" should "be tested" in {
    assert(PredicateTests.testingNotEqualityOnMapStringInteger(sc, PredicateTests.tableName))
  }

  "testingNotEqualityConditionOnSeq" should "be tested" in {
    assert(PredicateTests.testingNotEqualityConditionOnSeq(sc, PredicateTests.tableName))
  }

  "testingSizeOf" should "be tested" in {
    assert(PredicateTests.testingSizeOf(sc, PredicateTests.tableName))
  }

  "testingSizeOfWithComplexCondition" should "be tested" in {
    assert(PredicateTests.testingSizeOfWithComplexCondition(sc, PredicateTests.tableName))
  }

  "testingTypeOfWithNonExistantType" should "be tested" in {
    assert(PredicateTests.testingTypeOfWithNonExistantType(sc, PredicateTests.tableName))
  }

  "testingWithQueryCondition" should "be tested" in {
    assert(PredicateTests.testingWithQueryCondition(sc, PredicateTests.tableName))
  }

//  "testWithListINCondition" should "be tested" in {
//    assert(PredicateTests.testWithListINCondition(sc, PredicateTests.tableName))
//  }

  "testingSizeOfNotEquals" should "be tested" in {
    assert(PredicateTests.testingSizeOfNotEquals(sc, PredicateTests.tableName))
  }

  "testingINConditionOnSeqwithInSeq" should "be tested" in {
    assert(PredicateTests.testingINConditionOnSeqwithInSeq(sc, PredicateTests.tableName))
  }

  //The following tests are for sparkSql functionality
  "testingBooleanVsNull" should "be tested" in {
    assert(SparkSqlAccessTests.testingBooleanVsNull(spark, SparkSqlAccessTests.tableName))
  }

  "testingStringVsNull" should "be tested" in {
    assert(SparkSqlAccessTests.testingStringVsNull(spark, SparkSqlAccessTests.tableName))
  }

  "testingByteVsNull" should "be tested" in {
    assert(SparkSqlAccessTests.testingByteVsNull(spark, SparkSqlAccessTests.tableName))
  }

  "testingShortVsNull" should "be tested" in {
    assert(SparkSqlAccessTests.testingShortVsNull(spark, SparkSqlAccessTests.tableName))
  }

  "testingIntVsNull" should "be tested" in {
    assert(SparkSqlAccessTests.testingIntVsNull(spark, SparkSqlAccessTests.tableName))
  }

  "testingLongVsNull" should "be tested" in {
    assert(SparkSqlAccessTests.testingLongVsNull(spark, SparkSqlAccessTests.tableName))
  }

  "testingFloatVsNull" should "be tested" in {
    assert(SparkSqlAccessTests.testingFloatVsNull(spark, SparkSqlAccessTests.tableName))
  }

  "testingDoubleVsNull" should "be tested" in {
    assert(SparkSqlAccessTests.testingDoubleVsNull(spark, SparkSqlAccessTests.tableName))
  }

  "testingDateVsNull" should "be tested" in {
    assert(SparkSqlAccessTests.testingDateVsNull(spark, SparkSqlAccessTests.tableName))
  }

  "testingTimeVsNull" should "be tested" in {
    assert(SparkSqlAccessTests.testingTimeVsNull(spark, SparkSqlAccessTests.tableName))
  }

  "testingTimestampVsNull" should "be tested" in {
    assert(SparkSqlAccessTests.testingTimeStampVsNull(spark, SparkSqlAccessTests.tableName))
  }

  "testingBinaryVsNull" should "be tested" in {
    assert(SparkSqlAccessTests.testingBinaryVsNull(spark, SparkSqlAccessTests.tableName))
  }

  "testingMapVsNull" should "be tested" in {
    assert(SparkSqlAccessTests.testingMapVsNull(spark, SparkSqlAccessTests.tableName))
  }

  "testingArrayVsNull" should "be tested" in {
    assert(SparkSqlAccessTests.testingArrayVsNull(spark, SparkSqlAccessTests.tableName))
  }


  "testingBooleanVsString" should "be tested" in {
    assert(SparkSqlAccessTests.testingBooleanVsString(spark, SparkSqlAccessTests.tableName))
  }

  "testingBooleanVsByte" should "be tested" in {
    assert(SparkSqlAccessTests.testingBooleanVsByte(spark, SparkSqlAccessTests.tableName))
  }

  "testingBooleanVsShort" should "be tested" in {
    assert(SparkSqlAccessTests.testingBooleanVsShort(spark, SparkSqlAccessTests.tableName))
  }

  "testingBooleanVsInt" should "be tested" in {
    assert(SparkSqlAccessTests.testingBooleanVsInt(spark, SparkSqlAccessTests.tableName))
  }

  "testingBooleanVsLong" should "be tested" in {
    assert(SparkSqlAccessTests.testingBooleanVsLong(spark, SparkSqlAccessTests.tableName))
  }

  "testingBooleanVsFloat" should "be tested" in {
    assert(SparkSqlAccessTests.testingBooleanVsFloat(spark, SparkSqlAccessTests.tableName))
  }

  "testingBooleanVsDouble" should "be tested" in {
    assert(SparkSqlAccessTests.testingBooleanVsDouble(spark, SparkSqlAccessTests.tableName))
  }

  "testingBooleanVsDate" should "be tested" in {
    assert(SparkSqlAccessTests.testingBooleanVsDate(spark, SparkSqlAccessTests.tableName))
  }

  "testingBooleanVsTime" should "be tested" in {
    assert(SparkSqlAccessTests.testingBooleanVsTime(spark, SparkSqlAccessTests.tableName))
  }

  "testingBooleanVsTimestamp" should "be tested" in {
    assert(SparkSqlAccessTests.testingBooleanVsTimeStamp(spark, SparkSqlAccessTests.tableName))
  }

  "testingBooleanVsBinary" should "be tested" in {
    assert(SparkSqlAccessTests.testingBooleanVsBinary(spark, SparkSqlAccessTests.tableName))
  }

  "testingBooleanVsMap" should "be tested" in {
    assert(SparkSqlAccessTests.testingBooleanVsMap(spark, SparkSqlAccessTests.tableName))
  }

  "testingBooleanVsArray" should "be tested" in {
    assert(SparkSqlAccessTests.testingBooleanVsArray(spark, SparkSqlAccessTests.tableName))
  }

  "testingStringVsByte" should "be tested" in {
    assert(SparkSqlAccessTests.testingStringVsByte(spark, SparkSqlAccessTests.tableName))
  }

  "testingStringVsShort" should "be tested" in {
    assert(SparkSqlAccessTests.testingStringVsShort(spark, SparkSqlAccessTests.tableName))
  }

  "testingStringVsInt" should "be tested" in {
    assert(SparkSqlAccessTests.testingStringVsInt(spark, SparkSqlAccessTests.tableName))
  }

  "testingStringVsLong" should "be tested" in {
    assert(SparkSqlAccessTests.testingStringVsLong(spark, SparkSqlAccessTests.tableName))
  }

  "testingStringVsFloat" should "be tested" in {
    assert(SparkSqlAccessTests.testingStringVsFloat(spark, SparkSqlAccessTests.tableName))
  }

  "testingStringVsDouble" should "be tested" in {
    assert(SparkSqlAccessTests.testingStringVsDouble(spark, SparkSqlAccessTests.tableName))
  }

  "testingStringVsDate" should "be tested" in {
    assert(SparkSqlAccessTests.testingStringVsDate(spark, SparkSqlAccessTests.tableName))
  }

  "testingStringVsTime" should "be tested" in {
    assert(SparkSqlAccessTests.testingStringVsTime(spark, SparkSqlAccessTests.tableName))
  }

  "testingStringVsTimeStamp" should "be tested" in {
    assert(SparkSqlAccessTests.testingStringVsTimeStamp(spark, SparkSqlAccessTests.tableName))
  }

  "testingStringVsBinary" should "be tested" in {
    assert(SparkSqlAccessTests.testingStringVsBinary(spark, SparkSqlAccessTests.tableName))
  }

  "testingStringVsMap" should "be tested" in {
    assert(SparkSqlAccessTests.testingStringVsMap(spark, SparkSqlAccessTests.tableName))
  }

  "testingStringVsArray" should "be tested" in {
    assert(SparkSqlAccessTests.testingStringVsArray(spark, SparkSqlAccessTests.tableName))
  }

  "testingByteVsShort" should "be tested" in{
    assert(SparkSqlAccessTests.testingByteVsShort(spark, SparkSqlAccessTests.tableName))
  }

  "testingByteVsInt" should "be tested" in{
    assert(SparkSqlAccessTests.testingByteVsInt(spark, SparkSqlAccessTests.tableName))
  }

  "testingByteVsLong" should "be tested" in{
    assert(SparkSqlAccessTests.testingByteVsLong(spark, SparkSqlAccessTests.tableName))
  }

  "testingByteVsFloat" should "be tested" in{
    assert(SparkSqlAccessTests.testingByteVsFloat(spark, SparkSqlAccessTests.tableName))
  }

  "testingByteVsDouble" should "be tested" in{
    assert(SparkSqlAccessTests.testingByteVsDouble(spark, SparkSqlAccessTests.tableName))
  }

  "testingByteVsDate" should "be tested" in{
    assert(SparkSqlAccessTests.testingByteVsDate(spark, SparkSqlAccessTests.tableName))
  }

  "testingByteVsTime" should "be tested" in{
    assert(SparkSqlAccessTests.testingByteVsTime(spark, SparkSqlAccessTests.tableName))
  }

  "testingByteVsTimeStamp" should "be tested" in{
    assert(SparkSqlAccessTests.testingByteVsTimeStamp(spark, SparkSqlAccessTests.tableName))
  }

  "testingByteVsBinary" should "be tested" in{
    assert(SparkSqlAccessTests.testingByteVsBinary(spark, SparkSqlAccessTests.tableName))
  }

  "testingByteVsMap" should "be tested" in{
    assert(SparkSqlAccessTests.testingByteVsMap(spark, SparkSqlAccessTests.tableName))
  }

  "testingByteVsArray" should "be tested" in{
    assert(SparkSqlAccessTests.testingByteVsArray(spark, SparkSqlAccessTests.tableName))
  }

  "testingShortVsInt" should "be tested" in{
    assert(SparkSqlAccessTests.testingShortVsInt(spark, SparkSqlAccessTests.tableName))
  }

  "testingShortVsLong" should "be tested" in{
    assert(SparkSqlAccessTests.testingShortVsLong(spark, SparkSqlAccessTests.tableName))
  }

  "testingShortVsFloat" should "be tested" in{
    assert(SparkSqlAccessTests.testingShortVsFloat(spark, SparkSqlAccessTests.tableName))
  }

  "testingShortVsDouble" should "be tested" in{
    assert(SparkSqlAccessTests.testingShortVsDouble(spark, SparkSqlAccessTests.tableName))
  }

  "testingShortVsDate" should "be tested" in{
    assert(SparkSqlAccessTests.testingShortVsDate(spark, SparkSqlAccessTests.tableName))
  }

  "testingShortVsTime" should "be tested" in{
    assert(SparkSqlAccessTests.testingShortVsTime(spark, SparkSqlAccessTests.tableName))
  }

  "testingShortVsTimeStamp" should "be tested" in{
    assert(SparkSqlAccessTests.testingShortVsTimeStamp(spark, SparkSqlAccessTests.tableName))
  }

  "testingShortVsBinary" should "be tested" in{
    assert(SparkSqlAccessTests.testingShortVsBinary(spark, SparkSqlAccessTests.tableName))
  }

  "testingShortVsMap" should "be tested" in{
    assert(SparkSqlAccessTests.testingShortVsMap(spark, SparkSqlAccessTests.tableName))
  }

  "testingShortArray" should "be tested" in{
    assert(SparkSqlAccessTests.testingShortVsArray(spark, SparkSqlAccessTests.tableName))
  }

  "testingIntVsLong" should "be tested" in{
    assert(SparkSqlAccessTests.testingIntVsLong(spark, SparkSqlAccessTests.tableName))
  }

  "testingIntVsFloat" should "be tested" in{
    assert(SparkSqlAccessTests.testingIntVsFloat(spark, SparkSqlAccessTests.tableName))
  }

  "testingIntVsDouble" should "be tested" in{
    assert(SparkSqlAccessTests.testingIntVsDouble(spark, SparkSqlAccessTests.tableName))
  }

  "testingIntVsDate" should "be tested" in{
    assert(SparkSqlAccessTests.testingIntVsDate(spark, SparkSqlAccessTests.tableName))
  }

  "testingIntVsTime" should "be tested" in{
    assert(SparkSqlAccessTests.testingIntVsTime(spark, SparkSqlAccessTests.tableName))
  }

  "testingIntVsTimeStamp" should "be tested" in{
    assert(SparkSqlAccessTests.testingIntVsTimeStamp(spark, SparkSqlAccessTests.tableName))
  }

  "testingIntVsBinary" should "be tested" in{
    assert(SparkSqlAccessTests.testingIntVsBinary(spark, SparkSqlAccessTests.tableName))
  }

  "testingIntVsMap" should "be tested" in{
    assert(SparkSqlAccessTests.testingIntVsMap(spark, SparkSqlAccessTests.tableName))
  }

  "testingIntArray" should "be tested" in{
    assert(SparkSqlAccessTests.testingIntVsArray(spark, SparkSqlAccessTests.tableName))
  }


  "testingLongVsFloat" should "be tested" in{
    assert(SparkSqlAccessTests.testingLongVsFloat(spark, SparkSqlAccessTests.tableName))
  }

  "testingLongVsDouble" should "be tested" in{
    assert(SparkSqlAccessTests.testingLongVsDouble(spark, SparkSqlAccessTests.tableName))
  }

  "testingLongVsDate" should "be tested" in{
    assert(SparkSqlAccessTests.testingLongVsDate(spark, SparkSqlAccessTests.tableName))
  }

  "testingLongVsTime" should "be tested" in{
    assert(SparkSqlAccessTests.testingLongVsTime(spark, SparkSqlAccessTests.tableName))
  }

  "testingLongVsTimeStamp" should "be tested" in{
    assert(SparkSqlAccessTests.testingLongVsTimeStamp(spark, SparkSqlAccessTests.tableName))
  }

  "testingLongVsBinary" should "be tested" in{
    assert(SparkSqlAccessTests.testingLongVsBinary(spark, SparkSqlAccessTests.tableName))
  }

  "testingLongVsMap" should "be tested" in{
    assert(SparkSqlAccessTests.testingLongVsMap(spark, SparkSqlAccessTests.tableName))
  }

  "testingLongArray" should "be tested" in{
    assert(SparkSqlAccessTests.testingLongVsArray(spark, SparkSqlAccessTests.tableName))
  }

  "testingFloatVsDouble" should "be tested" in{
    assert(SparkSqlAccessTests.testingFloatVsDouble(spark, SparkSqlAccessTests.tableName))
  }

  "testingFloatVsDate" should "be tested" in{
    assert(SparkSqlAccessTests.testingFloatVsDate(spark, SparkSqlAccessTests.tableName))
  }

  "testingFloatVsTime" should "be tested" in{
    assert(SparkSqlAccessTests.testingFloatVsTime(spark, SparkSqlAccessTests.tableName))
  }

  "testingFloatVsTimeStamp" should "be tested" in{
    assert(SparkSqlAccessTests.testingFloatVsTimeStamp(spark, SparkSqlAccessTests.tableName))
  }

  "testingFloatVsBinary" should "be tested" in{
    assert(SparkSqlAccessTests.testingFloatVsBinary(spark, SparkSqlAccessTests.tableName))
  }

  "testingFloatVsMap" should "be tested" in{
    assert(SparkSqlAccessTests.testingFloatVsMap(spark, SparkSqlAccessTests.tableName))
  }

  "testingFloatArray" should "be tested" in{
    assert(SparkSqlAccessTests.testingFloatVsArray(spark, SparkSqlAccessTests.tableName))
  }

  "testingDoubleVsDate" should "be tested" in{
    assert(SparkSqlAccessTests.testingDoubleVsDate(spark, SparkSqlAccessTests.tableName))
  }

  "testingDoubleVsTime" should "be tested" in{
    assert(SparkSqlAccessTests.testingDoubleVsTime(spark, SparkSqlAccessTests.tableName))
  }

  "testingDoubleVsTimeStamp" should "be tested" in{
    assert(SparkSqlAccessTests.testingDoubleVsTimeStamp(spark, SparkSqlAccessTests.tableName))
  }

  "testingDoubleVsBinary" should "be tested" in{
    assert(SparkSqlAccessTests.testingDoubleVsBinary(spark, SparkSqlAccessTests.tableName))
  }

  "testingDoubleVsMap" should "be tested" in{
    assert(SparkSqlAccessTests.testingDoubleVsMap(spark, SparkSqlAccessTests.tableName))
  }

  "testingDoubleArray" should "be tested" in{
    assert(SparkSqlAccessTests.testingDoubleVsArray(spark, SparkSqlAccessTests.tableName))
  }

  "testingDateVsTime" should "be tested" in{
    assert(SparkSqlAccessTests.testingDateVsTime(spark, SparkSqlAccessTests.tableName))
  }

  "testingDateVsTimeStamp" should "be tested" in{
    assert(SparkSqlAccessTests.testingDateVsTimeStamp(spark, SparkSqlAccessTests.tableName))
  }

  "testingDateVsBinary" should "be tested" in{
    assert(SparkSqlAccessTests.testingDateVsBinary(spark, SparkSqlAccessTests.tableName))
  }

  "testingDateVsMap" should "be tested" in{
    assert(SparkSqlAccessTests.testingDateVsMap(spark, SparkSqlAccessTests.tableName))
  }

  "testingDateArray" should "be tested" in{
    assert(SparkSqlAccessTests.testingDateVsArray(spark, SparkSqlAccessTests.tableName))
  }


  "testingTimeVsTimeStamp" should "be tested" in{
    assert(SparkSqlAccessTests.testingTimeVsTimeStamp(spark, SparkSqlAccessTests.tableName))
  }

  "testingTimeVsBinary" should "be tested" in{
    assert(SparkSqlAccessTests.testingTimeVsBinary(spark, SparkSqlAccessTests.tableName))
  }

  "testingTimeVsMap" should "be tested" in{
    assert(SparkSqlAccessTests.testingTimeVsMap(spark, SparkSqlAccessTests.tableName))
  }

  "testingTimeArray" should "be tested" in{
    assert(SparkSqlAccessTests.testingTimeVsArray(spark, SparkSqlAccessTests.tableName))
  }

  "testingTimeStampVsBinary" should "be tested" in{
    assert(SparkSqlAccessTests.testingTimeStampVsBinary(spark, SparkSqlAccessTests.tableName))
  }

  "testingTimeStampVsMap" should "be tested" in{
    assert(SparkSqlAccessTests.testingTimeStampVsMap(spark, SparkSqlAccessTests.tableName))
  }

  "testingTimeStampArray" should "be tested" in{
    assert(SparkSqlAccessTests.testingTimeStampVsArray(spark, SparkSqlAccessTests.tableName))
  }

  "testingBinaryVsMap" should "be tested" in{
    assert(SparkSqlAccessTests.testingBinaryVsMap(spark, SparkSqlAccessTests.tableName))
  }

  "testingBinaryVsArray" should "be tested" in{
    assert(SparkSqlAccessTests.testingBinaryVsArray(spark, SparkSqlAccessTests.tableName))
  }

  "testingArrayVsMap" should "be tested" in{
    assert(SparkSqlAccessTests.testingArrayVsMap(spark, SparkSqlAccessTests.tableName))
  }

  "testFilterPushDownOnIdColumn" should "be tested" in {
    assert(SparkSqlPushDownTests.testFilterPushDownOnIdColumn(spark, SparkSqlPushDownTests.tableName))
  }

  "testFilterPushDownOnNonIDColumn" should "be tested" in {
    assert(SparkSqlPushDownTests.testFilterPushDownOnNonIDColumn(spark, SparkSqlPushDownTests.tableName))
  }

  "testFilterPushDownOnMapColumn" should "be tested" in  {
    assert(SparkSqlPushDownTests.testFilterPushDownOnMapColumn(spark, SparkSqlPushDownTests.tableName))
  }

  "testFilterPushDownOnArrayColumn" should " be tested" in {
    assert(SparkSqlPushDownTests.testFilterPushDownOnArrayColumn(spark, SparkSqlPushDownTests.tableName))
  }

  "testLTFilterPushDownOnNonIDColumn" should "be tested" in {
    assert(SparkSqlPushDownTests.testLTFilterPushDownOnNonIDColumn(spark, SparkSqlPushDownTests.tableName))
  }

  "testLTEFilterPushDownOnNonIDColumn" should "be tested" in {
    assert(SparkSqlPushDownTests.testLTEFilterPushDownOnNonIDColumn(spark, SparkSqlPushDownTests.tableName))
  }

  "testGTFilterPushDownOnNonIDColumn" should "be tested" in {
    assert(SparkSqlPushDownTests.testGTFilterPushDownOnNonIDColumn(spark, SparkSqlPushDownTests.tableName))
  }

  "testGTEFilterPushDownOnNonIDColumn" should "be tested" in {
    assert(SparkSqlPushDownTests.testGTEFilterPushDownOnNonIDColumn(spark, SparkSqlPushDownTests.tableName))
  }

  "testComplexOrFilterPushDownOnNonIDColumn" should "be tested" in {
    assert(SparkSqlPushDownTests.testComplexOrFilterPushDownOnNonIDColumn(spark, SparkSqlPushDownTests.tableName))
  }

  "testComplexAndFilterPushDownOnNonIDColumn" should "be tested" in {
    assert(SparkSqlPushDownTests.testComplexAndFilterPushDownOnNonIDColumn(spark, SparkSqlPushDownTests.tableName))
  }

  "testStartsWithPushDownOnNonIDColumn" should "be tested" in {
    assert(SparkSqlPushDownTests.testStartsWithPushDownOnNonIDColumn(spark, SparkSqlPushDownTests.tableName))
  }

  "testEndsWithPushDownOnNonIDColumn" should "be tested" in {
    assert(SparkSqlPushDownTests.testEndsWithPushDownOnNonIDColumn(spark, SparkSqlPushDownTests.tableName))
  }

  "testContainsPushDownOnNonIDColumn" should "be tested" in {
    assert(SparkSqlPushDownTests.testContainsPushDownOnNonIDColumn(spark, SparkSqlPushDownTests.tableName))
  }

  "testINPushDownOnNonIDColumn" should "be tested" in {
    assert(SparkSqlPushDownTests.testINPushDownOnNonIDColumn(spark, SparkSqlPushDownTests.tableName))
  }

  "testComplexNotOrFilterPushDownOnNonIDColumn" should "be tested" in {
    assert(SparkSqlPushDownTests.testComplexNotOrFilterPushDownOnNonIDColumn(spark, SparkSqlPushDownTests.tableName))
  }

  "testComplexNotAndFilterPushDownOnNonIDColumn" should "be tested" in {
    assert(SparkSqlPushDownTests.testComplexNotAndFilterPushDownOnNonIDColumn(spark, SparkSqlPushDownTests.tableName))
  }

  "testProjectionPushDown" should "be tested" in {
    assert(SparkSqlPushDownTests.testProjectionPushDown(spark, SparkSqlPushDownTests.tableName))
  }

  "testProjectionPushDownNextFields" should "be tested" in {
    assert(SparkSqlPushDownTests.testProjectionPushDownNestedFields(spark, SparkSqlPushDownTests.tableName))
  }

  "testFilterPDOnDFWithRDDFilter" should "be tested" in {
    assert(SparkSqlPushDownTests.testFilterPDOnDFWithRDDFilter(spark, SparkSqlPushDownTests.tableName))
  }

  "testProjectionPDOnDFWithRDDSelection" should "be tested" in {
    assert(SparkSqlPushDownTests.testProjectionPDOnDFWithRDDSelection(spark, SparkSqlPushDownTests.tableName))
  }

  "testProjectionPDOnDFWithRDDSelectionErrorCondition" should "be tested" in {
    assert(SparkSqlPushDownTests.testProjectionPDOnDFWithRDDSelectionErrorCondition(spark, SparkSqlPushDownTests.tableName))
  }

  "testingLoadExplicitSchema" should "be tested" in {
    assert(SparkSqlLoadAndSaveTests.testingLoadExplicitSchema(spark, SparkSqlLoadAndSaveTests.tableName))
  }

  "testingLoadBeanClass" should "be tested" in {
    assert(SparkSqlLoadAndSaveTests.testingLoadBeanClass(spark, SparkSqlLoadAndSaveTests.tableName))
  }

  "testingLoadInferSchema" should "be tested" in {
    assert(SparkSqlLoadAndSaveTests.testingLoadInferSchema(spark, SparkSqlLoadAndSaveTests.tableName))
  }

  "testingLoadFromDFReader" should "be tested" in {
    assert(SparkSqlLoadAndSaveTests.testingLoadFromDFReader(spark, SparkSqlLoadAndSaveTests.tableName))
  }

  "testingLoadFromDFReaderLoad" should "be tested" in {
    assert(SparkSqlLoadAndSaveTests.testingLoadFromDFReaderLoad(spark, SparkSqlLoadAndSaveTests.tableName))
  }

  "testingLoadFromDFWriterWithOperationOption" should "be tested" in {
    assert(SparkSqlLoadAndSaveTests.testingLoadFromDFWriterWithOperationOption(spark, SparkSqlLoadAndSaveTests.tableName))
  }

  "testingLoadFromDFReaderWithOperationOption" should "be tested" in {
    assert(SparkSqlLoadAndSaveTests.testingLoadFromDFReaderWithOperationOption(spark, SparkSqlLoadAndSaveTests.tableName))
  }

  "tesitngLoadFromDFReaderWithSampleOption" should "be tested" in {
    assert(SparkSqlLoadAndSaveTests.tesitngLoadFromDFReaderWithSampleOption(spark, SparkSqlLoadAndSaveTests.tableName))
  }

  "testingLoadFromDFReaderWithFailOnConflict" should "be tested" in {
    assert(SparkSqlLoadAndSaveTests.testingLoadFromDFReaderWithFailOnConflict(spark, SparkSqlLoadAndSaveTests.tableName))
  }

  "testingSaveFromSparkSession" should "be tested" in {
    assert(SparkSqlLoadAndSaveTests.testingSaveFromSparkSession(spark, SparkSqlLoadAndSaveTests.tableName))
  }

  "testingSaveFromDFWSession" should "be tested" in {
    assert(SparkSqlLoadAndSaveTests.testingSaveFromDFWSession(spark, SparkSqlLoadAndSaveTests.tableName))
  }

  "testingSaveWithBulkLoad" should "be tested" in {
    assert(SparkSqlLoadAndSaveTests.testingSaveWithBulkLoad(spark, SparkSqlLoadAndSaveTests.tableName))
  }

  "testingSaveWithComplexDocument" should "be tested" in {
    assert(SparkSqlLoadAndSaveTests.testingSaveWithComplexDocument(spark))
  }

  "testingUpdateToMapRDB" should "be tested" in {
    assert(SparkSqlLoadAndSaveTests.testingUpdateToMapRDB(spark, SparkSqlLoadAndSaveTests.tableName))
  }

  "testingCheckAndUpdateToMapRDB" should "be tested" in {
    assert(SparkSqlLoadAndSaveTests.testingCheckAndUpdateToMapRDB(spark, SparkSqlLoadAndSaveTests.tableName))
  }

  "testingUpdateToMapRDBAddToArray" should "be tested" in {
    assert(SparkSqlLoadAndSaveTests.testingUpdateToMapRDBAddToArray(spark, SparkSqlLoadAndSaveTests.tableName))
  }

//  "testSavingDStreamToMapRDBTable" should "be tested" in {
//    assert(SparkStreamingTests.testSavingDStreamToMapRDBTable(ssc, SparkStreamingTests.tableName))
//  }

  "testingSimpleSaveModeBeanClass" should "be tested" in {
    assert(LoadAndSaveTests.testingSimpleSaveModeBeanClass(spark.sparkContext, tableName, LoadAndSaveTests.saveToTable))
  }
}
