package databases.nosql.dynamodb

import datatofromapachespark.databases.nosql.dynamodb.DynamoDBRelation
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider}

class DefaultSource extends RelationProvider {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    new DynamoDBRelation(parameters,sqlContext)

  }

}