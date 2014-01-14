package io.colonelsanders.vertx.dynamodb.test;

import org.junit.Test;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

import java.io.IOException;

import static org.vertx.testtools.VertxAssert.assertEquals;
import static org.vertx.testtools.VertxAssert.assertTrue;

public class QueryTest extends TestBase {

    @Test
    public void testQueryOnSecondaryIndex() throws IOException {
        sendSample("QueryTest-secondaryIndex.json", new ResponseValidations() {
            @Override
            public void validateResponse(Message<JsonObject> event) {
                assertEquals("ok", event.body().getString("status"));
                assertEquals(1, event.body().getArray("results").size());
                JsonObject result = event.body().getArray("results").get(0);
                assertEquals("bd999faf-9be9-43ee-b079-7f1bf708d28b", result.getString("guid"));
            }
        });
    }

    @Test
    public void testQueryKeyBeginsWith() throws IOException {
        sendSample("QueryTest-keyBeginsWith.json", new ResponseValidations() {
            @Override
            public void validateResponse(Message<JsonObject> event) {
                assertEquals("error", event.body().getString("status"));
                assertTrue(event.body().getString("message").contains("condition not supported"));
            }
        });
    }

    @Test
    public void testQueryKeyContains() throws IOException {
        sendSample("QueryTest-keyContains.json", new ResponseValidations() {
            @Override
            public void validateResponse(Message<JsonObject> event) {
                assertEquals("error", event.body().getString("status"));
                assertTrue(event.body().getString("message").contains("conditional constraint is not an indexable operation"));
            }
        });
    }
}
