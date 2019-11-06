package databases.nosql.dynamodb

import datatofromapachespark.databases.nosql.dynamodb.ScanPartition
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType


class DynamoDBRDD(sc:SparkContext,schema: StructType,scanPartitions: Seq[ScanPartition],tableName:String,numPartitions:Int) extends RDD[Row](sc,Nil){

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
      val scanPartition = split.asInstanceOf[ScanPartition]
      scanPartition.scanTable(numPartitions)

  }

  override protected def getPartitions: Array[Partition] = scanPartitions.toArray
}
