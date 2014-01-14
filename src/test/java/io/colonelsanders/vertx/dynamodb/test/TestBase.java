package io.colonelsanders.vertx.dynamodb.test;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;

import java.io.*;

import static org.vertx.testtools.VertxAssert.assertTrue;
import static org.vertx.testtools.VertxAssert.testComplete;

public abstract class TestBase extends TestVerticle {

    public static final String ADDRESS = "test.mod-dynamodb";

    /**
     * Deploy an instance of the DynamoDB vertx for test. AWS credentials are loaded from a properties file in the user's
     * home directory.
     */
    @Override
    public void start() {
        initialize();
        container.deployModule(System.getProperty("vertx.modulename"), buildConfig(), new AsyncResultHandler<String>() {
            @Override
            public void handle(AsyncResult<String> event) {
                assertTrue(event.succeeded());
                startTests();
            }
        });
    }

    private JsonObject buildConfig() {
        JsonObject config = new JsonObject();
        config.putString("address", ADDRESS);
        File credentials = new File(System.getProperty("user.home"), "AwsCredentials.properties");
        config.putString("awsCredentialsFile", credentials.getPath());
        return config;
    }

    /**
     * Load a sample JSON file from the classpath.
     */
    protected JsonObject loadSample(String name) throws IOException {
        StringWriter json = new StringWriter();
        BufferedReader in = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(name)));
        String line;
        while ((line = in.readLine()) != null) {
            json.append(line).append("\n");
        }
        return new JsonObject(json.toString());
    }

    /**
     * Invoke the DynamoDB vertx with sample data and verify the results.
     */
    protected void sendSample(String sampleName, final ResponseValidations validations) throws IOException {
        JsonObject sample = loadSample(sampleName);
        vertx.eventBus().send(ADDRESS, sample, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> event) {
                try {
                    validations.validateResponse(event);
                } finally {
                    testComplete();
                }
            }
        });
    }

    /**
     * Encapsulates assertions against the return value from the vertx.
     */
    public interface ResponseValidations {
        void validateResponse(Message<JsonObject> event);
    }
}
