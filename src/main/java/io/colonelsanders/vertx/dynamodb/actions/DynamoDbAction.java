package io.colonelsanders.vertx.dynamodb.actions;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Container;

public interface DynamoDbAction {
    void doAction(Container vertxContainer, AmazonDynamoDBAsync dbClient, Message<JsonObject> message);
}
