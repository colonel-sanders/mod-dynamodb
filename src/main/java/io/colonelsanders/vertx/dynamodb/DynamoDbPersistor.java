package io.colonelsanders.vertx.dynamodb;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import io.colonelsanders.vertx.dynamodb.json.JsonConverter;
import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DynamoDbPersistor extends BusModBase implements Handler<Message<JsonObject>> {

    protected ExecutorService dbExecutor;
    protected AmazonDynamoDBAsync dbClient;

    @Override
    public void handle(Message<JsonObject> event) {
        String actionVal = getMandatoryString("action", event);
        if (actionVal != null) {
            Action action;
            try {
                action = Action.valueOf(actionVal);
            } catch (IllegalArgumentException e) {
                sendError(event, "Unrecognized action " + actionVal);
                return;
            }

            try {
                initiateAction(action, event);
            } catch (Exception e) {
                sendError(event, e.getMessage(), e);
            }
        }
    }

    protected void initiateAction(Action action, Message<JsonObject> message) {
        switch (action) {
            case GetItem:
                doGetItem(message); break;
            case Query:
                doQuery(message); break;
        }
    }

    private void doGetItem(final Message<JsonObject> message) {
        String table = message.body().getString("table");
        GetItemRequest request = new GetItemRequest()
                .withTableName(table)
                .withKey(JsonConverter.attributesFromJson(message.body().getObject("document")));
        dbClient.getItemAsync(request, new AsyncHandler<GetItemRequest, GetItemResult>() {
            @Override
            public void onError(Exception exception) {
                logger.error("GetItem failed", exception);
                fail(message, exception);
            }

            @Override
            public void onSuccess(GetItemRequest request, GetItemResult result) {
                message.reply(new JsonObject()
                        .putString("status", "ok")
                        .putObject("result", JsonConverter.resultToJson(result.getItem())));
            }
        });
    }

    private static void fail(Message<JsonObject> message, Exception exception) {
        message.reply(new JsonObject().putString("status", "error").putString("message", "GetItem failed: " + exception));
    }

    private void doQuery(final Message<JsonObject> message) {
        String table = message.body().getString("table");
        String index = message.body().getString("index");
        QueryRequest request = new QueryRequest();
        if (table != null) {
            request.setTableName(table);
        }
        if (index != null) {
            request.setIndexName(index);
        }
        request.setKeyConditions(JsonConverter.conditionFromJson(message.body().getObject("document")));

        dbClient.queryAsync(request, new AsyncHandler<QueryRequest, QueryResult>() {
            @Override
            public void onError(Exception exception) {
                logger.error("Query failed", exception);
                fail(message, exception);
            }

            @Override
            public void onSuccess(QueryRequest request, QueryResult queryResult) {
                message.reply(new JsonObject()
                        .putString("status", "ok")
                        .putArray("results", JsonConverter.resultsToJson(queryResult.getItems())));
            }
        });
    }

    @Override
    public void start() {
        super.start();
        if (dbExecutor == null || dbClient == null) {
            AwsConfigHelper configHelper = new AwsConfigHelper(config, logger);
            dbExecutor = Executors.newCachedThreadPool();
            dbClient = new AmazonDynamoDBAsyncClient(configHelper.configureCredentials(), configHelper.configureClient(), dbExecutor);
        }
        String address = getOptionalStringConfig("address", "vertx.dynamodb");
        eb.registerHandler(address, this);
    }

    @Override
    public void stop() {
        if (dbExecutor != null) {
            dbExecutor.shutdown();
            try {
                dbExecutor.awaitTermination(15, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {}
            if (dbClient != null) {
                dbClient.shutdown();
            }
        }
        super.stop();
    }
}
