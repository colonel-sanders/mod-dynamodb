package io.colonelsanders.vertx.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import io.colonelsanders.vertx.dynamodb.actions.*;
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
        DynamoDbAction executor = null;
        switch (action) {
            case GetItem:
                executor = new GetItemAction(); break;
            case Query:
                executor = new QueryAction(); break;
            case Scan:
                executor = new ScanAction(); break;
            case UpdateItem:
                executor = new UpdateItemAction(); break;
            case DeleteItem:
                executor = new DeleteItemAction(); break;
        }
        executor.doAction(container, dbClient, message);
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
