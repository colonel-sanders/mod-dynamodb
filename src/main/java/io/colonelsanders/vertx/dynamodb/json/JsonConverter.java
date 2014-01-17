package io.colonelsanders.vertx.dynamodb.json;

import com.amazonaws.services.dynamodbv2.model.*;
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
                attribs.put(e.getKey(), parseAttributeValue(e.getValue()));
            }
        }
        return attribs;
    }

    public static Map<String, AttributeValueUpdate> attributeUpdatesFromJson(JsonObject object) {
        HashMap<String,AttributeValueUpdate> attribs = new HashMap<>();
        if (object != null) {
            for (Map.Entry<String,Object> e : object.toMap().entrySet()) {
                attribs.put(e.getKey(), parseAttributeValueUpdate(e.getValue()));
            }
        }
        return attribs;
    }

    /**
     * Not supported yet: list types, binary types, IN conditions, probably other conditions.
     */
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
        condition.setAttributeValueList(Collections.singleton(parseAttributeValue(e.getValue())));
        return condition;
    }

    private static Condition parseComplexCondition(Map<?,?> complexCondition, String field) {
        if (complexCondition.size() != 1) {
            throw new IllegalArgumentException("Complex conditional for field " + field + " should contain exactly one condition");
        }
        Object op = complexCondition.keySet().iterator().next();
        Object filter = complexCondition.values().iterator().next();

        Condition condition = new Condition();
        ComparisonOperator comp = operatorNamed(op);
        condition.setComparisonOperator(comp);
        if (comp != ComparisonOperator.NULL && comp != ComparisonOperator.NOT_NULL) {
            condition.setAttributeValueList(Collections.singleton(parseAttributeValue(filter)));
        }
        return condition;
    }

    private static AttributeValue parseAttributeValue(Object value) {
        if (value instanceof String) {
            return new AttributeValue((String) value);
        }
        if (value instanceof Map) {
            return parseAttributeValueMap((Map) value);
        }
        return parseAttributeValueMap(((JsonObject) value).toMap());
    }

    private static AttributeValue parseAttributeValueMap(Map<?,?> value) {
        AttributeValue attributeValue = new AttributeValue();
        if (value.get("N") != null) {
            attributeValue.setN(value.get("N").toString());
        } else {
            attributeValue.setS(value.values().iterator().next().toString());
        }
        return attributeValue;
    }

    private static AttributeValueUpdate parseAttributeValueUpdate(Object value) {
        if (value instanceof String) {
            return new AttributeValueUpdate(parseAttributeValue(value), AttributeAction.PUT);
        } else if (value instanceof Map) {
            return parseAttributeValueUpdateMap((Map) value);
        }
        return parseAttributeValueUpdateMap(((JsonObject) value).toMap());
    }

    private static AttributeValueUpdate parseAttributeValueUpdateMap(Map<?,?> value) {
        if (value.size() != 1) {
            throw new IllegalArgumentException("Attribute update value should contain exactly one operator");
        }

        AttributeAction action = actionNamed(value.keySet().iterator().next());
        if (action == null) {
            return new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(parseAttributeValueMap(value));
        }

        Object val = value.values().iterator().next();
        return new AttributeValueUpdate()
                .withAction(actionNamed(action))
                .withValue(parseAttributeValue(val));
    }

    private static ComparisonOperator operatorNamed(Object value) {
        for (ComparisonOperator op : ComparisonOperator.values()) {
            if (op.name().equals(value.toString().toUpperCase())) {
                return op;
            }
        }
        return null;
    }

    private static AttributeAction actionNamed(Object value) {
        for (AttributeAction action : AttributeAction.values()) {
            if (action.name().equals(value.toString().toUpperCase())) {
                return action;
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

    public static List<JsonObject> resultsToJson(List<Map<String,AttributeValue>> results) {
        ArrayList<JsonObject> array = new ArrayList<>();
        for (Map<String, AttributeValue> result : results)
        {
            array.add(JsonConverter.resultToJson(result));
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
