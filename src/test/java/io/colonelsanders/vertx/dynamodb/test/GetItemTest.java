package io.colonelsanders.vertx.dynamodb.test;

import org.junit.Test;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

import java.io.IOException;

import static org.vertx.testtools.VertxAssert.assertEquals;
import static org.vertx.testtools.VertxAssert.assertTrue;

public class GetItemTest extends TestBase {

    @Test
    public void testRecordFound() throws IOException {
        sendSample("GetItemTest-recordFound.json", new ResponseValidations() {
            @Override
            public void validateResponse(Message<JsonObject> event) {
                assertEquals("ok", event.body().getString("status"));
                assertEquals("FY 1997", event.body().getObject("result").getString("year"));
            }
        });
    }

    @Test
    public void testRecordNotFound() throws IOException {
        sendSample("GetItemTest-recordNotFound.json", new ResponseValidations() {
            @Override
            public void validateResponse(Message<JsonObject> event) {
                assertEquals("ok", event.body().getString("status"));
                assertTrue(event.body().getObject("result").size() == 0);
            }
        });
    }

    @Test
    public void testIndexNotGiven() throws IOException {
        sendSample("GetItemTest-indexNotGiven.json", new ResponseValidations() {
            @Override
            public void validateResponse(Message<JsonObject> event) {
                assertEquals("error", event.body().getString("status"));
                assertTrue(event.body().getString("message").contains("ValidationException"));
            }
        });
    }

    /**
     * Secondary indexes are not eligible for a GetItem call; use Query instead.
     */
    @Test
    public void testSecondaryIndex() throws IOException {
        sendSample("GetItemTest-secondaryIndex.json", new ResponseValidations() {
            @Override
            public void validateResponse(Message<JsonObject> event) {
                assertEquals("error", event.body().getString("status"));
                assertTrue(event.body().getString("message").contains("ValidationException"));
            }
        });
    }
}
