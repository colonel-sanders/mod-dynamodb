package io.colonelsanders.vertx.dynamodb.test;

import org.junit.Test;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

import java.io.IOException;

import static org.vertx.testtools.VertxAssert.assertEquals;
import static org.vertx.testtools.VertxAssert.assertTrue;

public class UpdateItemTest extends TestBase {
    @Test
    public void testSuccess() throws IOException {
        sendSample("UpdateItemTest-success.json", new ResponseValidations() {
            @Override
            public void validateResponse(Message<JsonObject> event) {
                assertEquals("ok", event.body().getString("status"));
            }
        });
    }

    @Test
    public void testChangePrimaryKey() throws IOException {
        sendSample("UpdateItemTest-changePrimaryKey.json", new ResponseValidations() {
            @Override
            public void validateResponse(Message<JsonObject> event) {
                assertEquals("error", event.body().getString("status"));
                assertTrue(event.body().getString("message").contains("This attribute is part of the key"));
            }
        });
    }
}
