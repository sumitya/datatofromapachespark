package datatofromapachespark.databases.nosql

import org.apache.spark.sql.SparkSession

object NoSqlReader {

  def readFromDynamoDB(spark: SparkSession) ={

    /*1. Validate the keys.
    * 2. query operation
    * 3. scan operation.
    * 4. item operations
    */

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
