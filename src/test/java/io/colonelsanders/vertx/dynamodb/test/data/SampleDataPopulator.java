package io.colonelsanders.vertx.dynamodb.test.data;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.util.Tables;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class SampleDataPopulator implements AsyncHandler<PutItemRequest,PutItemResult> {

    public static final String TABLE_NAME = "us_currency";

    private final AmazonDynamoDBAsync client;
    private final List<Exception> failures;

    public SampleDataPopulator(AmazonDynamoDBAsync client) {
        this.client = client;
        this.failures = Collections.synchronizedList(new ArrayList<Exception>());
    }

    public void createTestTable() {
        if (tableExists()) {
            System.out.println("Table " + TABLE_NAME + " already exists");
            return;
        }
        System.out.println("Creating table " + TABLE_NAME);
        KeySchemaElement pk = new KeySchemaElement("guid", KeyType.HASH);
        GlobalSecondaryIndex year = new GlobalSecondaryIndex()
                .withIndexName("year-index")
                .withKeySchema(new KeySchemaElement("year", KeyType.HASH))
                .withProjection(new Projection().withProjectionType(ProjectionType.KEYS_ONLY))
                .withProvisionedThroughput(new ProvisionedThroughput(10L, 5L));
        CreateTableRequest create = new CreateTableRequest()
                .withTableName(TABLE_NAME)
                .withKeySchema(pk)
                .withGlobalSecondaryIndexes(year)
                .withAttributeDefinitions(new AttributeDefinition("guid", ScalarAttributeType.S), new AttributeDefinition("year", ScalarAttributeType.S))
                .withProvisionedThroughput(new ProvisionedThroughput(10L, 5L));
        client.createTable(create);
        Tables.waitForTableToBecomeActive(client, TABLE_NAME);
        System.out.println("Table " + TABLE_NAME + " active");
    }

    private boolean tableExists() {
        try {
            DescribeTableResult desc = client.describeTable(new DescribeTableRequest(TABLE_NAME));
            String status = desc.getTable().getTableStatus();
            if (status.equals(TableStatus.DELETING.toString())) {
                throw new IllegalStateException("Table " + TABLE_NAME + " is currently being deleted");
            }
            if (!status.equals(TableStatus.ACTIVE.toString())) {
                Tables.waitForTableToBecomeActive(client, TABLE_NAME);
            }
            return true;
        } catch (ResourceNotFoundException e) {
            return false;
        }
    }

    public void populateTestTableData() throws IOException {
        InputStream currencyData = getClass().getResourceAsStream("us-currency-printing.json");
        ObjectNode root = new ObjectMapper().readValue(currencyData, ObjectNode.class);
        currencyData.close();

        Map<String,Integer> columnIndexes = new HashMap<>();
        JsonNode columnMeta = root.get("meta").get("view").get("columns");
        for (int i=0; i < columnMeta.size(); i++) {
            columnIndexes.put(columnMeta.get(i).get("fieldName").asText(), i);
        }

        System.out.println("Populating table with test data");
        JsonNode data = root.get("data");
        for (int i=0; i < data.size(); i++) {
            Map<String, AttributeValue> item = new HashMap<>();
            String guid = data.get(i).get(columnIndexes.get(":id")).asText().toLowerCase();
            item.put("guid", new AttributeValue().withS(guid));
            String year = data.get(i).get(columnIndexes.get("fiscal_year")).asText();
            item.put("year", new AttributeValue().withS(year));
            for (Map.Entry<String,String> e : columnNameToPropertyName().entrySet()) {
                int column = columnIndexes.get(e.getKey());
                long val = parseNumeric(data.get(i).get(column).asText());
                item.put(e.getValue(), new AttributeValue().withN(String.valueOf(val)));
            }
            client.putItemAsync(new PutItemRequest().withTableName(TABLE_NAME).withItem(item), this);
        }
    }

    private static HashMap<String,String> columnNameToPropertyName() {
        HashMap<String,String> m = new HashMap<>();
        m.put("_1_bills", "ones");
        m.put("_2_bills", "twos");
        m.put("_5_bills", "fives");
        m.put("_10_bills", "tens");
        m.put("_20_bills", "twenties");
        m.put("_50_bills", "fifties");
        m.put("_100_bills", "hundreds");
        return m;
    }

    private static long parseNumeric(String value) {
        try {
            return value == null ? 0 : Long.parseLong(value.replaceAll(",", ""));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    @Override
    public void onError(Exception e) {
        failures.add(e);
    }

    @Override
    public void onSuccess(PutItemRequest putItemRequest, PutItemResult putItemResult) {}

    public List<Exception> getFailures() {
        synchronized (failures) {
            return new ArrayList<>(failures);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        String accessKey = args[0];
        String secretKey = args[1];
        AmazonDynamoDBAsyncClient client = new AmazonDynamoDBAsyncClient(new BasicAWSCredentials(accessKey, secretKey));
        SampleDataPopulator dataPopulator = new SampleDataPopulator(client);
        dataPopulator.createTestTable();
        dataPopulator.populateTestTableData();

        System.out.println("Waiting for completion");
        client.getExecutorService().shutdown();
        client.getExecutorService().awaitTermination(5L, TimeUnit.MINUTES);

        if (!dataPopulator.getFailures().isEmpty()) {
            System.err.println("There were failures when writing test data:");
            for (Exception e : dataPopulator.getFailures()) {
                System.err.println(e);
            }
        }
    }
}
