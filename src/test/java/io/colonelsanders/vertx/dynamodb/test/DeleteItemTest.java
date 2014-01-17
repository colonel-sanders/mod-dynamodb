package io.colonelsanders.vertx.dynamodb.test;

import org.junit.Test;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

import java.io.IOException;

import static org.vertx.testtools.VertxAssert.assertEquals;
import static org.vertx.testtools.VertxAssert.assertTrue;

public class DeleteItemTest extends TestBase {

    @Test
    public void testRecordFound() throws IOException {
        final JsonObject sampleItemGet = loadSample("DeleteItemTest-found.json");
        sampleItemGet.putString("action", "GetItem");
        sampleItemGet.putBoolean("consistent_read", true);

        sendSample("DeleteItemTest-found.json", new ResponseValidations() {
            @Override
            public void validateResponse(Message<JsonObject> event) {
                assertEquals("ok", event.body().getString("status"));

                //validate that the record is gone
                vertx.eventBus().send(ADDRESS, sampleItemGet, new Handler<Message<JsonObject>>() {
                    @Override
                    public void handle(Message<JsonObject> response) {
                        assertEquals("ok", response.body().getString("status"));
                        assertTrue(response.body().getObject("result").size() == 0);
                    }
                });
            }
        });
    }

    @Test
    public void testRecordNotFound() throws IOException {
        sendSample("DeleteItemTest-notFound.json", new ResponseValidations() {
            @Override
            public void validateResponse(Message<JsonObject> event) {
                assertEquals("ok", event.body().getString("status"));
            }
        });
    }
}
