package io.colonelsanders.vertx.dynamodb.actions;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.handlers.AsyncHandler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;

import java.util.ArrayList;
import java.util.List;

public abstract class PaginatedResultHandler<REQUEST extends AmazonWebServiceRequest, RESULT> implements AsyncHandler<REQUEST,RESULT> {

    protected Message<JsonObject> message;
    protected Logger logger;
    protected List<JsonObject> results;

    protected PaginatedResultHandler(Message<JsonObject> message, Logger logger) {
        this.message = message;
        this.logger = logger;
        this.results = new ArrayList<>();
    }

    abstract void firstPage(REQUEST request);
    abstract void nextPage(REQUEST lastRequest, RESULT lastResult);
    abstract boolean hasMorePages(RESULT lastResult);
    abstract List<JsonObject> resultToJson(RESULT result);

    @Override
    public void onSuccess(REQUEST request, RESULT result) {
        results.addAll(resultToJson(result));
        if (hasMorePages(result)) {
            nextPage(request, result);
        } else {
            JsonArray array = new JsonArray();
            for (JsonObject o : results) {
                array.addObject(o);
            }
            message.reply(new JsonObject()
                    .putString("status", "ok")
                    .putArray("results", array));
        }
    }

    @Override
    public void onError(Exception exception) {
        logger.error("Processing failed for message " + message, exception);
        message.reply(new JsonObject().putString("status", "error").putString("message", exception.toString()));
    }
}
