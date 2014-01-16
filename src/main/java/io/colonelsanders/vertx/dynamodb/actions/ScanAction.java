package io.colonelsanders.vertx.dynamodb.actions;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import io.colonelsanders.vertx.dynamodb.json.JsonConverter;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.platform.Container;

import java.util.List;

/**
 * No support for segmented (parallel) scans yet.
 */
public class ScanAction implements DynamoDbAction {

    @Override
    public void doAction(Container vertxContainer, AmazonDynamoDBAsync dbClient, Message<JsonObject> message) {
        String table = message.body().getString("table");
        ScanRequest request = new ScanRequest()
                .withTableName(table);
        if (message.body().getObject("filter") != null) {
            request.setScanFilter(JsonConverter.conditionFromJson(message.body().getObject("filter")));
        }

        new PaginatedScanResultHandler(message, dbClient, vertxContainer.logger()).firstPage(request);
    }

    private static class PaginatedScanResultHandler extends PaginatedResultHandler<ScanRequest,ScanResult> {

        private AmazonDynamoDBAsync dbClient;

        private PaginatedScanResultHandler(Message<JsonObject> message, AmazonDynamoDBAsync dbClient, Logger logger) {
            super(message, logger);
            this.dbClient = dbClient;
        }

        @Override
        void firstPage(ScanRequest request) {
            dbClient.scanAsync(request, this);
        }

        @Override
        void nextPage(ScanRequest lastRequest, ScanResult lastResult) {
            lastRequest.setExclusiveStartKey(lastResult.getLastEvaluatedKey());
            dbClient.scanAsync(lastRequest, this);
        }

        @Override
        boolean hasMorePages(ScanResult lastResult) {
            return lastResult.getLastEvaluatedKey() != null && !lastResult.getLastEvaluatedKey().isEmpty();
        }

        @Override
        List<JsonObject> resultToJson(ScanResult scanResult) {
            return JsonConverter.resultsToJson(scanResult.getItems());
        }
    }
}
