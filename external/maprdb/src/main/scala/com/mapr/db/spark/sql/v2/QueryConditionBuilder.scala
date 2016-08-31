package com.mapr.db.spark.sql.v2

import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources._
import org.ojai.store.{Connection, QueryCondition}

object QueryConditionBuilder extends Logging {

  import com.mapr.db.spark.sql.v2.QueryConditionExtensions._

  import collection.JavaConversions._

  def buildQueryConditionFrom(filters: List[Filter])(implicit connection: Connection): String =
    createFilterCondition(filters).asJsonString()

  def addTabletInfo(queryJson: String, queryCondition: String): String =
    if (queryJson == "{}") {
      queryCondition
    } else {
      "{\"$and\":[" + queryJson + "," + queryCondition + "]}"
    }

  /**
    * Spark sends individual filters down that we need to concat using AND. This function evaluates each filter
    * recursively and creates the corresponding OJAI query.
    *
    * @param filters
    * @param connection
    * @return
    */
  private def createFilterCondition(filters: List[Filter])(implicit connection: Connection): QueryCondition = {
    log.debug(s"FILTERS TO PUSH DOWN: $filters")

    val andCondition = connection.newCondition().and()

    val finalCondition = filters
      .foldLeft(andCondition) { (partialCondition, filter) => partialCondition.condition(evalFilter(filter)) }
      .close()
      .build()

    log.debug(s"FINAL OJAI QUERY CONDITION: ${finalCondition.toString}")

    finalCondition
  }

  /**
    * Translate a Spark Filter to an OJAI query.
    *
    * It recursively translate nested filters.
    *
    * @param filter
    * @param connection
    * @return
    */
  private def evalFilter(filter: Filter)(implicit connection: Connection): QueryCondition = {

    log.debug("evalFilter: " + filter.toString)

    val condition = filter match {

      case Or(left, right) => connection.newCondition()
        .or()
        .condition(evalFilter(left))
        .condition(evalFilter(right))
        .close()
        .build()

      case And(left, right) => connection.newCondition()
        .and()
        .condition(evalFilter(left))
        .condition(evalFilter(right))
        .close()
        .and()

      case singleFilter => evalSingleFilter(singleFilter)
    }

    condition
  }

  private def evalSingleFilter(filter: Filter)(implicit connection: Connection) = {

    val simpleCondition = filter match {
      case IsNull(field)                  => connection.newCondition().notExists(field)
      case IsNotNull(field)               => connection.newCondition().exists(field)
      case In(field, values)              => connection.newCondition().in(field, values.toList)
      case StringStartsWith(field, value) => connection.newCondition().matches(field, value)
      case EqualTo(field, value)          => connection.newCondition().field(field) === value
      case LessThan(field, value)         => connection.newCondition().field(field) < value
      case LessThanOrEqual(field, value)  => connection.newCondition().field(field) <= value
      case GreaterThan(field, value) => connection.newCondition.field(field) > value
      case GreaterThanOrEqual(field, value) => connection.newCondition.field(field) >= value
    }

    log.debug("evalSingleFilter: " + filter.toString + " =============== " + simpleCondition.toString)

    simpleCondition.build()
  }
}