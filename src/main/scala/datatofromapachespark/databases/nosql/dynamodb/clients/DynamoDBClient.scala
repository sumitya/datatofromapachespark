package datatofromapachespark.databases.nosql.dynamodb.clients

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.auth.{AWSCredentialsProviderChain, EnvironmentVariableCredentialsProvider}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBClientBuilder}

private[dynamodb] object DynamoDBClient {

  def getDynamoDB(region: String ): DynamoDB = {
    getDynamoDBClient(region)
    val client: AmazonDynamoDB = getDynamoDBClient(region)

    new DynamoDB(client)

  }

  private def getDynamoDBClient(region: String ):AmazonDynamoDB= {

    val chosenRegion = region
    val credentials = getAWSCredentails()

    Option(System.getProperty("aws.dynamodb.endpoint")).map(endpoint => {
      AmazonDynamoDBClientBuilder.standard()
        .withCredentials(credentials)
        .withEndpointConfiguration(new EndpointConfiguration(endpoint, chosenRegion))
        .build()
    }).getOrElse(
      AmazonDynamoDBClientBuilder.standard()
        .withCredentials(credentials)
        .withRegion(chosenRegion)
        .build()
    )

  }

  private def getAWSCredentails():AWSCredentialsProviderChain = {
    val awsCredentials = new AWSCredentialsProviderChain(new EnvironmentVariableCredentialsProvider(), new ProfileCredentialsProvider())
    awsCredentials
  }
}
