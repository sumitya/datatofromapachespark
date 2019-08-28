package datatofromapachespark.databases.rdbms

import org.apache.spark.sql.{DataFrame, SparkSession}

class RdbmsReader {

  def readFromMySql(spark: SparkSession): DataFrame = {

  /*
  * Below spark.read.option(...).load() method reads the schema,get partition info and
  * reads the actual data of databases.rdbms table via internal object JDBCRelation:
  *   1. reads the schema in getSchema(...) method [[org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation]]
  *   2. reads the partition info. columnPartition(...) [[org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation]]
  *   3. reads the data in buildScan(...) method [[org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation]]
  *       JDBCRDD - An RDD representing a table in a database accessed via JDBC.  Both the
  *          driver code and the workers must be able to access the database; the driver
  *          needs to fetch the schema while the workers need to fetch the data.
  *     a. scanTable(...) method in [[org.apache.spark.sql.execution.datasources.jdbc.JDBCRDD]]
  *         Build and return JDBCRDD from the given information.
  *     b. compute(...) method in [[org.apache.spark.sql.execution.datasources.jdbc.JDBCRDD]]
  *         Runs the SQL query against the JDBC driver.
  * */

  import datatofromapachespark.databases._

  val jdbcDF = spark.read
    .format("jdbc")
    .option("dbtable", DBTABLE)
    .option("url", URL)
    .option("user", USER)
    .option("password", PASSWORD)
    .option("numPartitions", NUMPARTITIONS)
    .option("partitionColumn", PARTITIONCOLUMN)
    .option("lowerBound", LOWERBOUND)
    .option("upperBound", UPPERBOUND)
    .option("fetchsize",FETCHSIZE)
    .load()

  jdbcDF

  }

  def readFromRedShift(spark: SparkSession) = {}

}
