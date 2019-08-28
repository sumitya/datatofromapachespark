package datatofromapachespark.databases.nosql.dynamodb.tables

import com.amazonaws.services.dynamodbv2.model.{KeySchemaElement, KeyType}

case class TableKeys(hashKeyName: String, rangeKeyName: Option[String])

object TableKeys {

  def fromDescription(keySchemaElements: Seq[KeySchemaElement]): TableKeys = {

    val hashKeyName = keySchemaElements.find(_.getKeyType == KeyType.HASH.toString).get.getAttributeName
    val rangeKeyName = keySchemaElements.find(_.getKeyType == KeyType.RANGE.toString).map(_.getAttributeName)

    TableKeys(hashKeyName, rangeKeyName)

  }
}
