package com.mapr.db.spark.sql.v2

import org.apache.hadoop.fs.{Path, PathFilter}
import org.ojai.store.DriverManager

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType


package object MapRDBSpark {

  implicit class SessionOps(sparkSession: SparkSession) {
    /**
     * Entry point to the DataSource API
     *
     * @param path   MapR Table path. Notice that the path can be a Hadoop FS pattern.
     *               For example:
     *
     *               Given the MFS tables: /clients/client_1/data.table and
     *               /clients/client_2/data.table
     *
     *               we could call [[loadFromMapRDB]] with path clients\/\*\/\*.table and it will load each individual
     *               table and return an Union DataFrame.
     *
     *               Notice that each individual table must have the same [[StructType]]
     * @param schema Schema to be read.
     * @param many   How many readers per tablet.
     * @return
     */
    def loadFromMapRDB(path: String, schema: StructType, many: Int = 1): DataFrame = {

      if (path.contains("*")) {
        val paths = expand(path)

        if (paths.nonEmpty) {
          loadUnionFromMapRDB(paths: _*)(schema, many)
        } else {
          sparkSession.emptyDataFrame
        }
      } else {
        sparkSession
          .read
          .format("com.mapr.db.spark.sql.v2.Reader")
          .schema(schema)
          .option("readers", many)
          .load(path)
      }
    }

    private def loadUnionFromMapRDB(paths: String*)(schema: StructType, many: Int = 1): DataFrame =
      paths
        .map(path => loadFromMapRDB(path, schema, many))
        .reduce { (a, b) =>
          if (a.schema != b.schema) {
            throw new Exception(s"Table Schema does not match. ${a.schema} != ${b.schema}")
          } else {
            a.union(b)
          }
        }

    private def expand(path: String): Seq[String] =
      FileOps.fs
        .globStatus(new Path(path), FileOps.dbPathFilter)
        .map(_.getPath.toString)
  }

}

private object FileOps {

  import org.apache.hadoop.conf.Configuration
  import org.apache.hadoop.fs.{FileSystem, Path}

  lazy val fs: FileSystem = {
    val conf = new Configuration()

    FileSystem.get(conf)
  }

  lazy val dbPathFilter: PathFilter = new PathFilter {
    private lazy val connection = DriverManager.getConnection("ojai:mapr:")

    override def accept(path: Path): Boolean = connection.storeExists(path.toString)
  }
}