package io.colonelsanders.vertx.dynamodb.test.data;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import org.junit.rules.ExternalResource;

import java.io.File;

public class SampleDataExternalResource extends ExternalResource {

    @Override
    protected void before() throws Throwable {
        File awsCredentialsFile = new File(System.getProperty("user.home"), "AwsCredentials.properties");
        AmazonDynamoDBClient dbClient = new AmazonDynamoDBClient(new PropertiesCredentials(awsCredentialsFile));
        SampleDataPopulator populator = new SampleDataPopulator(dbClient);
        populator.createTestTable();
        populator.clearTestTableData();
        populator.populateTestTableData();
    }
}
