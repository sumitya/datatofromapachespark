package datatofromapachespark.databases.nosql.dynamodb

import databases.nosql.dynamodb.DynamoDBRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._
import datatofromapachespark.databases.nosql.dynamodb.operations.DynamoDBTable
import datatofromapachespark.utils.Contexts

import scala.collection.JavaConverters._

class DynamoDBRelation(parameters: Map[String, String],val sqlContext: SQLContext)  extends
  BaseRelation with TableScan{

  val tableName = parameters("tableName")

  val numPartitions:Int = parameters.get("readPartitions").map(_.toInt).getOrElse(sqlContext.sparkContext.defaultParallelism)

  val schema: StructType = interSchemaFromTable

  override def buildScan(): RDD[Row] = {

    new DynamoDBRDD(sqlContext.sparkContext,schema,(0 until numPartitions).map(index => new ScanPartition(index,tableName)),tableName:String,numPartitions)

  }

  private def interSchemaFromTable:StructType = {

    val itemSelectable = new DynamoDBTable(0,tableName).scan(numPartitions).firstPage().getLowLevelResult.getItems.asScala

    val jsonSchema = itemSelectable.map(_.toJSON)

    val jsonDS = Contexts.SQL_CONTEXT.sparkContext.parallelize(jsonSchema)

    val jsonDF = Contexts.SQL_CONTEXT.read.json(jsonDS)

    jsonDF.schema

  }

  private def inferType(value: Any): DataType = value match {
    case number: java.math.BigDecimal =>
      if (number.scale() == 0) {
        if (number.precision() < 10) IntegerType
        else if (number.precision() < 19) LongType
        else DataTypes.createDecimalType(number.precision(), number.scale())
      }
      else DoubleType
    case list: java.util.ArrayList[_] =>
      if (list.isEmpty) ArrayType(StringType)
      else ArrayType(inferType(list.get(0)))
    case set: java.util.Set[_] =>
      if (set.isEmpty) ArrayType(StringType)
      else ArrayType(inferType(set.iterator().next()))
    case map: java.util.Map[String, _] =>
      val mapFields = (for ((fieldName, fieldValue) <- map.asScala) yield {
        StructField(fieldName, inferType(fieldValue))
      }).toSeq
      StructType(mapFields)
    case _: java.lang.Boolean => BooleanType
    case _: Array[Byte] => BinaryType
    case _ => StringType
  }

}
