package datatofromapachespark.databases.nosql.dynamodb

import com.amazonaws.services.dynamodbv2.document.Item
import datatofromapachespark.databases.nosql.dynamodb.operations.DynamoDBTable
import org.apache.spark.Partition
import org.apache.spark.sql.Row

import scala.annotation.tailrec
import scala.collection.JavaConverters._

class ScanPartition(partitionIndex:Int,tableName:String,requiredColumns: Seq[String] = Seq.empty) extends Partition{

  //full scan .i.e. with all the columns.
  def scanTable(numPartitions:Int): Iterator[Row] = {

    val scanResult = new DynamoDBTable(partitionIndex,tableName).scan(numPartitions)

    val pageIterator = scanResult.pages().iterator().asScala

    new Iterator[Row] {

      var innerIterator: Iterator[Row] = Iterator.empty

      @tailrec
      override def hasNext: Boolean = innerIterator.hasNext || {
        if (pageIterator.hasNext) {
          nextPage()
          hasNext
        }
        else false
      }

      override def next(): Row = innerIterator.next()

      private def nextPage(): Unit = {
        // Limit throughput to provisioned capacity.
        val page = pageIterator.next()
        innerIterator = page.getLowLevelResult.getItems.iterator().asScala.map(covertDDBItemToRow(_))

      }
    }
  }

  override def index: Int = partitionIndex

  def covertDDBItemToRow(item:Item):Row = {
    Row.fromSeq(item.asMap().asScala.values.toSeq)
  }
}
