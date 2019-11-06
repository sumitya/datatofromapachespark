package datatofromapachespark

import datatofromapachespark.databases.nosql.NoSqlReader
import datatofromapachespark.databases.rdbms.RdbmsReader
import datatofromapachespark.transformations.RDDTransformations
import datatofromapachespark.utils.{Contexts, GetAllProperties}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

object DataToFromApacheSparkApp {

  /* Since "no. of cores = no. of tasks" on an executor.
  *   Here, by default no. of connections are 1.
  *
  *  1. If master local[n] is set, then n no. of connections are created:
  *    a. If below options are specified, then n connections are created
  *     .option("numPartitions", 100) //No of splits to read
  *     .option("partitionColumn", "addressid")
  *     .option("lowerBound", 1)
  *     .option("upperBound", 100) //No. of records to be in every partition (upperBound/numPartitions), last partition will have remaining data.
  *
  *       i. If no. of executors < no. of partitions,
  *           Since no. of partitions are equal to no. of task,
  *           then other tasks has to wait to process partitions.
  *        ii. If no. of executors > no. of partitions
  *            Executors will be sitting idle.
  *
  *   b. If above options are not specified, then only a single connection is created.
  *
  *  2. spark-shell command runs as follows:
  *   spark-shell --master local[4]  --driver-class-path C:\Users\<USER_NAME>\.m2\repository\mysql\mysql-connector-java\5.1.46\mysql-connector-java-5.1.46.jar
  *
  *
  */

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.INFO)
    //Logger.getLogger("com").setLevel(Level.DEBUG)

    //get the current logged in user.
    val userName = System.getProperty("user.name")

    // get output file locations
    var parquetOutputPath = GetAllProperties.readPropertyFile get "PARQUET_OUTPUT_PATH" getOrElse("#") replace("<USER_NAME>",userName)
    var csvOutputPath = GetAllProperties.readPropertyFile get "CSV_OUTPUT_PATH" getOrElse("#") replace("<USER_NAME>",userName)

    val spark = Contexts.SPARK
/*
    val jdbcDF = new RdbmsReader().readFromMySql(spark)

    //Get no. of records per partition
    RDDTransformations.showRecordsPerPartitionMysql(jdbcDF,spark)

    //If partitionColumn is not a primary key. Repartition the data because of unbalance in partition.
    val jdbcDFRepartitioned = jdbcDF.repartition(10)

    RDDTransformations.showRecordsPerPartitionMysql(jdbcDFRepartitioned,spark)

    jdbcDF.createOrReplaceTempView("ADDRESS")

    jdbcDF.show(10)
    //spark.sql("select * from ADDRESS limit 10").show()

    jdbcDF.write.mode(SaveMode.Overwrite).parquet(parquetOutputPath)

    jdbcDF.write.mode(SaveMode.Overwrite).csv(csvOutputPath)

    /*
    **(1) Scan JDBCRelation(address) [numPartitions=2] [addressid#0,contactid#1,line1#2,city#3,state#4,zip#5] PushedFilters: [], ReadSchema: struct<addressid:int,contactid:int,line1:string,city:string,state:string,zip:string>
    */
    println(jdbcDF.queryExecution.executedPlan)

    // Below produces output like: Relation[addressid#0,contactid#1,line1#2,city#3,state#4,zip#5] JDBCRelation(address) [numPartitions=2]
    println(jdbcDF.queryExecution.optimizedPlan)

   // Below few print stmts gives metadata information about jdbcDF [[DataFrame]]
    println(jdbcDF.queryExecution.sparkPlan)

    println(jdbcDF.queryExecution.analyzed)

    println(jdbcDF.queryExecution.logical)

    println(jdbcDF.queryExecution.toString())

    println(jdbcDF.queryExecution.stringWithStats)

    println(jdbcDF.queryExecution.debug.codegen())

    //read from NoSql DB i.e. DynamoDB database.

    */
    val dynamoDBDataFrame  = NoSqlReader.readFromDynamoDB(spark)

    dynamoDBDataFrame.show()

    RDDTransformations.getCountByKeyDynamoDB(dynamoDBDataFrame,spark)

    //stop the sparkSession
    Contexts.stopContext
}

}
