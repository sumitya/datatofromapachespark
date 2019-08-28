package databases.nosql.dynamodb.operations

import com.amazonaws.services.dynamodbv2.document.{ItemCollection, ScanOutcome}

trait DynamoDBOperations {

  def scan(numPartitions:Int):ItemCollection[ScanOutcome]

}
