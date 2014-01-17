package io.colonelsanders.vertx.dynamodb.actions;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import io.colonelsanders.vertx.dynamodb.json.JsonConverter;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.platform.Container;

/**
 * No conditional update support yet.
 */
public class DeleteItemAction implements DynamoDbAction {
    @Override
    public void doAction(Container vertxContainer, AmazonDynamoDBAsync dbClient, final Message<JsonObject> message) {
        String table = message.body().getString("table");
        DeleteItemRequest request = new DeleteItemRequest()
                .withTableName(table)
                .withKey(JsonConverter.attributesFromJson(message.body().getObject("key")));

        final Logger logger = vertxContainer.logger();
        dbClient.deleteItemAsync(request, new AsyncHandler<DeleteItemRequest, DeleteItemResult>() {
            @Override
            public void onError(Exception exception) {
                logger.error("UpdateItem failed", exception);
                message.reply(new JsonObject()
                        .putString("status", "error")
                        .putString("message", exception.toString()));
            }

            @Override
            public void onSuccess(DeleteItemRequest request, DeleteItemResult updateItemResult) {
                message.reply(new JsonObject()
                        .putString("status", "ok"));
            }
        });
    }
}
