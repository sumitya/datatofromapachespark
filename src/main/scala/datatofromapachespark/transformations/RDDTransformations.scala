package datatofromapachespark.transformations

import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

object RDDTransformations {


  def showRecordsPerPartition(df:DataFrame,spark:SparkSession):Unit = {

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

}
