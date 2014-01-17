package io.colonelsanders.vertx.dynamodb.test.data;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.util.Tables;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class SampleDataPopulator {

    public static final String TABLE_NAME = "us_currency";

    private final AmazonDynamoDB client;

    public SampleDataPopulator(AmazonDynamoDB client) {
        this.client = client;
    }

    public void createTestTable() {
        if (tableExists()) {
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

    public void clearTestTableData() {
        System.out.println("Clearing existing test data");

        //get all item keys, 25 at a time
        List<List<String>> guids = new ArrayList<>();
        ScanRequest scanRequest = new ScanRequest()
                .withTableName(TABLE_NAME)
                .withLimit(25)
                .withAttributesToGet("guid");
        ScanResult result;
        do {
            result = client.scan(scanRequest);
            List<String> setGuids = new ArrayList<>(result.getCount());
            for (Map<String,AttributeValue> row : result.getItems()) {
                setGuids.add(row.get("guid").getS());
            }
            guids.add(setGuids);
            if (result.getLastEvaluatedKey() != null && !result.getLastEvaluatedKey().isEmpty()) {
                scanRequest.setExclusiveStartKey(result.getLastEvaluatedKey());
            }
        } while (result.getLastEvaluatedKey() != null && !result.getLastEvaluatedKey().isEmpty());

        if (!guids.isEmpty() && !guids.get(0).isEmpty()) {
            //delete all items, 25 at a time
            for (List<String> setGuids : guids) {
                List<WriteRequest> deletes = new ArrayList<>();
                for (String guid : setGuids) {
                    deletes.add(new WriteRequest(
                            new DeleteRequest(Collections.singletonMap("guid", new AttributeValue().withS(guid)))));
                }
                client.batchWriteItem(new BatchWriteItemRequest(Collections.singletonMap(TABLE_NAME, deletes)));
            }
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
            client.putItem(new PutItemRequest().withTableName(TABLE_NAME).withItem(item));
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

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            System.err.println("Specify either username/password or path to credentials properties file");
            return;
        }
        AWSCredentials credentials;
        if (args.length == 1) {
            credentials = new PropertiesCredentials(new File(args[0]));
        } else {
            String accessKey = args[0];
            String secretKey = args[1];
            credentials = new BasicAWSCredentials(accessKey, secretKey);
        }
        AmazonDynamoDBClient client = new AmazonDynamoDBClient(credentials);
        SampleDataPopulator populator = new SampleDataPopulator(client);
        populator.createTestTable();
        populator.clearTestTableData();
        populator.populateTestTableData();
    }
}
