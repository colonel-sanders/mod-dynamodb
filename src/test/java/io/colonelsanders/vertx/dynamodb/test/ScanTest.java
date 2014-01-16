package io.colonelsanders.vertx.dynamodb.test;

import org.junit.Test;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

import java.io.IOException;

import static org.vertx.testtools.VertxAssert.assertEquals;

public class ScanTest extends TestBase {
    @Test
    public void testFullScan() throws IOException {
        sendSample("ScanTest-fullScan.json", new ResponseValidations() {
            @Override
            public void validateResponse(Message<JsonObject> event) {
                assertEquals(33, event.body().getArray("results").size());
            }
        });
    }

    @Test
    public void testFilteredScan() throws IOException {
        sendSample("ScanTest-filtered.json", new ResponseValidations() {
            @Override
            public void validateResponse(Message<JsonObject> event) {
                assertEquals(28, event.body().getArray("results").size());
            }
        });
    }

    @Test
    public void testNoSuchColumn() throws IOException {
        sendSample("ScanTest-noSuchColumn.json", new ResponseValidations() {
            @Override
            public void validateResponse(Message<JsonObject> event) {
                assertEquals(0, event.body().getArray("results").size());
            }
        });
    }

    @Test
    public void testNotNull() throws IOException {
        sendSample("ScanTest-notNull.json", new ResponseValidations() {
            @Override
            public void validateResponse(Message<JsonObject> event) {
                assertEquals(33, event.body().getArray("results").size());
            }
        });
    }
}
