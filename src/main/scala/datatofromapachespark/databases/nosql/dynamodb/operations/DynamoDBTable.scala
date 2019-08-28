package datatofromapachespark.databases.nosql.dynamodb.operations

import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec
import com.amazonaws.services.dynamodbv2.document.{ItemCollection, ScanOutcome}
import com.amazonaws.services.dynamodbv2.model.{ReturnConsumedCapacity, TableDescription}
import com.google.common.util.concurrent.RateLimiter
import databases.nosql.dynamodb.operations.DynamoDBOperations
import datatofromapachespark.databases.STRONGLYCONSISTENTREADS
import datatofromapachespark.databases.nosql.dynamodb.clients.DynamoDBClient
import datatofromapachespark.databases.nosql.dynamodb.tables.TableKeys

import scala.collection.JavaConverters._

class DynamoDBTable(partitionIndex:Int,tableName:String,requiredColumns: Seq[String] = Seq.empty) extends DynamoDBOperations {

  val (keySchema, readLimit, writeLimit, itemLimit, totalSizeInBytes) = {

    import datatofromapachespark.databases._

    val table = DynamoDBClient.getDynamoDB(AWSREGION).getTable(tableName)
    val desc = table.describe()

    new TableDescription().getKeySchema

    // Key schema.
    val keySchema = TableKeys.fromDescription(desc.getKeySchema.asScala)

    // Parameters.
    val bytesPerRCU = BYTESPERRCU.toInt
    val targetCapacity = TARGETCAPACITY.toDouble
    val readFactor = if (STRONGLYCONSISTENTREADS.toBoolean) 1 else 2

    // Provisioned or on-demand throughput.
    val readThroughput = Option(desc.getProvisionedThroughput.getReadCapacityUnits)
      .filter(_ > 0).map(_.longValue())
      .getOrElse(100L)
    val writeThroughput = Option(desc.getProvisionedThroughput.getWriteCapacityUnits)
      .filter(_ > 0).map(_.longValue())
      .getOrElse(100L)

    // Rate limit calculation.
    val tableSize = desc.getTableSizeBytes
    val avgItemSize = tableSize.toDouble / desc.getItemCount
    val readCapacity = readThroughput * targetCapacity
    val writeCapacity = writeThroughput * targetCapacity

    val totalSegments = NOOFPARITIIONS.toInt
    val readLimit = readCapacity / totalSegments
    val itemLimit = ((bytesPerRCU / avgItemSize * readLimit).toInt * readFactor) max 1

    val writeLimit = writeCapacity / totalSegments

    (keySchema, readLimit, writeLimit, itemLimit, tableSize.toLong)

  }

  override def scan(numPartitions:Int): ItemCollection[ScanOutcome] = {

    val scanSpec = new ScanSpec()
      .withSegment(partitionIndex)
      .withTotalSegments(numPartitions)
      .withMaxPageSize(itemLimit)
      .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
      .withConsistentRead(STRONGLYCONSISTENTREADS.toBoolean)

    import datatofromapachespark.databases._

    val dynamoClient = DynamoDBClient.getDynamoDB(AWSREGION)

    val rateLimiter = RateLimiter.create(readLimit)

    dynamoClient.getTable(tableName).scan(scanSpec)

  }

}
