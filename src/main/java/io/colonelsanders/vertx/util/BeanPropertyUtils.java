package io.colonelsanders.vertx.util;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

public class BeanPropertyUtils {

    /**
     * List the writable bean properties for the given class. Returns empty sets rather than raising exceptions.
     */
    public static Set<PropertyDescriptor> writableProperties(Class<?> clazz) {
        HashSet<PropertyDescriptor> descriptors = new HashSet<>();
        try {
            Introspector.flushFromCaches(clazz);
            for (PropertyDescriptor p : Introspector.getBeanInfo(clazz).getPropertyDescriptors()) {
                if (p.getWriteMethod() != null && p.getWriteMethod().isAccessible()) {
                    descriptors.add(p);
                }
            }
        } catch (IntrospectionException e) {
            //by contract, don't explode
        }
        return descriptors;
    }

    public static void applyProperty(Object target, PropertyDescriptor property, String value) {
        try {
            Method writeMethod = property.getWriteMethod();
            writeMethod.invoke(target, convertValue(writeMethod.getParameterTypes()[0], value));
        } catch (IllegalAccessException | InvocationTargetException | ArrayIndexOutOfBoundsException e) {
            //by contract, don't explode
        }
    }

    @SuppressWarnings("unchecked")
    private static Object convertValue(Class<?> type, String value) {
        if (type.isPrimitive() && type.equals(Integer.TYPE)) {
            return Integer.parseInt(value);
        }
        if (type.isEnum()) {
            Class<? extends Enum> e = (Class<? extends Enum>) type;
            return Enum.valueOf(e, value);
        }
        return value;
    }
}
