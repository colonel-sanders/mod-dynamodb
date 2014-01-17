package io.colonelsanders.vertx.dynamodb.actions;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
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
public class UpdateItemAction implements DynamoDbAction {
    @Override
    public void doAction(Container vertxContainer, AmazonDynamoDBAsync dbClient, final Message<JsonObject> message) {
        String table = message.body().getString("table");
        UpdateItemRequest request = new UpdateItemRequest()
                .withTableName(table)
                .withKey(JsonConverter.attributesFromJson(message.body().getObject("key")));
        if (message.body().getObject("attributes") != null) {
            request.setAttributeUpdates(JsonConverter.attributeUpdatesFromJson(message.body().getObject("attributes")));
        }

        final Logger logger = vertxContainer.logger();
        dbClient.updateItemAsync(request, new AsyncHandler<UpdateItemRequest, UpdateItemResult>() {
            @Override
            public void onError(Exception exception) {
                logger.error("UpdateItem failed", exception);
                message.reply(new JsonObject()
                        .putString("status", "error")
                        .putString("message", exception.toString()));
            }

            @Override
            public void onSuccess(UpdateItemRequest request, UpdateItemResult updateItemResult) {
                message.reply(new JsonObject()
                        .putString("status", "ok"));
            }
        });
    }
}
