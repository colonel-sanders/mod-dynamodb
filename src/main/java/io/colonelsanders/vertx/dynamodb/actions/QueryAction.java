package io.colonelsanders.vertx.dynamodb.actions;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import io.colonelsanders.vertx.dynamodb.json.JsonConverter;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.platform.Container;

import java.util.List;

public class QueryAction implements DynamoDbAction {

    @Override
    public void doAction(Container vertxContainer, AmazonDynamoDBAsync dbClient, Message<JsonObject> message) {
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

        new PaginatedQueryResultHandler(dbClient, message, vertxContainer.logger()).firstPage(request);
    }

    private static class PaginatedQueryResultHandler extends PaginatedResultHandler<QueryRequest,QueryResult> {

        private AmazonDynamoDBAsync dbClient;

        private PaginatedQueryResultHandler(AmazonDynamoDBAsync dbClient, Message<JsonObject> message, Logger logger) {
            super(message, logger);
            this.dbClient = dbClient;
        }

        @Override
        void firstPage(QueryRequest request) {
            dbClient.queryAsync(request, this);
        }

        @Override
        void nextPage(QueryRequest lastRequest, QueryResult lastResult) {
            lastRequest.setExclusiveStartKey(lastResult.getLastEvaluatedKey());
            dbClient.queryAsync(lastRequest, this);
        }

        @Override
        boolean hasMorePages(QueryResult lastResult) {
            return lastResult.getLastEvaluatedKey() != null && !lastResult.getLastEvaluatedKey().isEmpty();
        }

        @Override
        List<JsonObject> resultToJson(QueryResult queryResult) {
            return JsonConverter.resultsToJson(queryResult.getItems());
        }
    }
}
