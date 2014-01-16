package io.colonelsanders.vertx.dynamodb.actions;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import io.colonelsanders.vertx.dynamodb.json.JsonConverter;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.platform.Container;

public class GetItemAction implements DynamoDbAction {
    @Override
    public void doAction(Container vertxContainer, AmazonDynamoDBAsync dbClient, final Message<JsonObject> message) {
        final Logger logger = vertxContainer.logger();
        String table = message.body().getString("table");
        GetItemRequest request = new GetItemRequest()
                .withTableName(table)
                .withKey(JsonConverter.attributesFromJson(message.body().getObject("key")));
        dbClient.getItemAsync(request, new AsyncHandler<GetItemRequest, GetItemResult>() {
            @Override
            public void onError(Exception exception) {
                logger.error("GetItem failed", exception);
                message.reply(new JsonObject()
                        .putString("status", "error")
                        .putString("message", exception.toString()));
            }

            @Override
            public void onSuccess(GetItemRequest request, GetItemResult result) {
                message.reply(new JsonObject()
                        .putString("status", "ok")
                        .putObject("result", JsonConverter.resultToJson(result.getItem())));
            }
        });
    }
}
