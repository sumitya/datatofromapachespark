package datatofromapachespark.databases.nosql

import org.apache.spark.sql.{DataFrameReader, SparkSession}

object NoSqlReader {

  def readFromDynamoDB(spark: SparkSession) ={

    /*1. Validate the keys.
    * 2. query operation
    * 3. scan operation.
    * 4. item operations
    */

    //reader reads the data from the DefaultSource

    import datatofromapachespark.databases._

    val reader = spark.
        read.
       format("databases.nosql.dynamodb")
      .option("tableName", DYNAMODBTABLE)
      .option("readPartitions",NOOFPARITIIONS.toInt)
      .load()

    reader

  }

  def readFromCassandra(spark: SparkSession)={
    //TODO: read according to the partitions

  }

  def readFromHbase(): Unit ={
    //TODO: read according to the keys

  }
  def readfromMongoDB(): Unit ={
    //TODO: read according to the keys

  }

}
