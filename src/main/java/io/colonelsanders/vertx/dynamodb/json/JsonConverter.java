package io.colonelsanders.vertx.dynamodb.json;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.*;

/**
 * Converts JSON messages to the AWS object model, and vice-versa.
 */
public class JsonConverter {

    public static Map<String, AttributeValue> attributesFromJson(JsonObject object) {
        HashMap<String,AttributeValue> attribs = new HashMap<>();
        if (object != null) {
            for (Map.Entry<String,Object> e : object.toMap().entrySet()) {
                attribs.put(e.getKey(), new AttributeValue(e.getValue().toString()));
            }
        }
        return attribs;
    }

    public static Map<String,Condition> conditionFromJson(JsonObject doc) {
        Map<String,Condition> conditions = new HashMap<>();
        for (Map.Entry<String,Object> e : doc.toMap().entrySet()) {
            Condition condition;
            String field = e.getKey();
            if (e.getValue() instanceof Map) {
                condition = parseComplexCondition((Map) e.getValue(), field);
            } else {
                condition = parseSimpleCondition(e);
            }
            conditions.put(field, condition);
        }
        return conditions;
    }

    private static Condition parseSimpleCondition(Map.Entry<String, Object> e) {
        Condition condition;
        condition = new Condition();
        condition.setComparisonOperator(ComparisonOperator.EQ);
        AttributeValue value = new AttributeValue(e.getValue().toString());
        condition.setAttributeValueList(Collections.singleton(value));
        return condition;
    }

    private static Condition parseComplexCondition(Map<?,?> complexCondition, String field) {
        if (complexCondition.size() != 1) {
            throw new IllegalArgumentException("Complex conditional for field " + field + " should contain exactly one condition");
        }
        Object op = complexCondition.keySet().iterator().next();
        Object filter = complexCondition.entrySet().iterator().next();

        Condition condition = new Condition();
        condition.setComparisonOperator(operatorNamed(op));
        AttributeValue value = new AttributeValue(filter.toString());
        condition.setAttributeValueList(Collections.singleton(value));
        return condition;
    }

    private static ComparisonOperator operatorNamed(Object value) {
        for (ComparisonOperator op : ComparisonOperator.values()) {
            if (op.name().equals(value.toString().toUpperCase())) {
                return op;
            }
        }
        return null;
    }

    public static JsonObject resultToJson(Map<String, AttributeValue> result) {
        JsonObject object = new JsonObject();
        if (result != null) {
            for (Map.Entry<String,AttributeValue> e : result.entrySet()) {
                object.putString(e.getKey(), valueOf(e.getValue()));
            }
        }
        return object;
    }

    public static JsonArray resultsToJson(List<Map<String,AttributeValue>> results) {
        JsonArray array = new JsonArray();
        for (Map<String, AttributeValue> result : results)
        {
            array.addObject(JsonConverter.resultToJson(result));
        }
        return array;
    }

    private static String valueOf(AttributeValue attribute) {
        if (attribute.getS() != null) {
            return attribute.getS();
        }
        if (attribute.getN() != null) {
            return attribute.getN();
        }
        return attribute.toString();
    }

}
