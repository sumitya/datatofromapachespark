package datatofromapachespark.transformations

import org.apache.spark.sql.{DataFrame, SparkSession}

object RDDTransformations {


  def showRecordsPerPartitionMysql(df:DataFrame,spark:SparkSession):Unit = {

    val partitionsRDD = df.rdd.mapPartitionsWithIndex{

      (partitionId,iterOfrows) => Iterator(iterOfrows.size)

    }

  /* Here, implicits object gives implicit conversions for converting Scala objects (incl. RDDs)
   * into a Dataset, DataFrame, Columns or supporting such conversions
  */

    val sqlContext = spark.sqlContext

    import sqlContext.implicits._

    partitionsRDD.toDF().show(100)

}

  def getCountByKeyDynamoDB(df:DataFrame,spark:SparkSession) = {


    /*val table = DynamoDBClient.getDynamoDB(AWSREGION).getTable(tableName)
    val desc = table.describe()

    new TableDescription().getKeySchema

    // Key schema.
    val keySchema = TableKeys.fromDescription(desc.getKeySchema.asScala)
    */



  }

}
