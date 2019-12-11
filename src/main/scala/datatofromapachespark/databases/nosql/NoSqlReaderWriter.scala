package datatofromapachespark.databases.nosql

import org.apache.spark.sql.{DataFrameReader, SparkSession}

object NoSqlReaderWriter {

  def readFromDynamoDB(spark: SparkSession) ={

    /*1. Validate the keys.
    * 2. query operation
    * 3. scan operation.
    * 4. item operations
    */

    //reader reads the data from the DefaultSource

    import datatofromapachespark.databases._

    val reader = spark.
        read.
       format("databases.nosql.dynamodb")
      .option("tableName", DYNAMODBTABLE)
      .option("readPartitions",NOOFPARITIIONS.toInt)
      .load()

    reader

  }

  def readFromCassandra(spark: SparkSession)={
    //TODO: read according to the partitions

    spark.conf.set("spark.cassandra.connection.host", "localhost")

    import com.datastax.spark.connector._

    val data = spark.sparkContext.cassandraTable("test","emp")

    data.map(row => row.getInt("emp_id")).stats()

  }

  def readFromHbase(): Unit ={
    //TODO: read according to the keys

  }
  def readfromMongoDB(): Unit ={
    //TODO: read according to the keys

  }


  def writeToCassandra(spark: SparkSession) = {

    spark.conf.set("spark.cassandra.connection.host", "localhost")

    import com.datastax.spark.connector._

    //@TODO: write custom output
    val rdd = spark.sparkContext.parallelize(List(Seq("moremagic", 1)))
    rdd.saveToCassandra("test" , "emp", SomeColumns("key", "value"))
    println("Written to cassandra!!!")

  }

}
