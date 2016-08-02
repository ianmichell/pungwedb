package com.pungwe.db.core.utils;

import net.jpountz.xxhash.StreamingXXHash64;
import net.jpountz.xxhash.XXHashFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Created by 917903 on 14/06/2016.
 */
public class Utils {

    public static long createHash(byte[] bytes) {
        XXHashFactory factory = XXHashFactory.fastestInstance();
        StreamingXXHash64 hash64 = factory.newStreamingHash64(0);
        hash64.update(bytes, 0, bytes.length);
        return hash64.getValue();
    }

    public static boolean hasKey(String field, Map<String, ?> value) {
        return false;
    }

    public static Optional<?> getValue(String field, Map<String, ?> value) {
        if (value.containsKey(field)) {
            return Optional.of(value.get(field));
        }
        // Split via dot notation...
        String[] fieldSplit = field.split("\\.");
        Object found = getValue(fieldSplit, value);
        if (found == null) {
            return Optional.empty();
        }
        return Optional.of(found);
    }

    private static Object getValue(String[] fields, Map<?, ?> value) {
        Object found = value.get(fields[0]);
        // If it's null, we don't want it...
        if (found == null) {
            return null;
        }
        if (Collection.class.isAssignableFrom(found.getClass()) && fields.length > 1) {
            // We only want the values if they are a map.
            Collection<Map<?, ?>> values = ((Collection<?>)found).stream().filter(o -> o != null &&
                    Map.class.isAssignableFrom(o.getClass())).map(o -> (Map<?, ?>)o).collect(Collectors.toList());
            if (values.isEmpty()) {
                return null;
            }
            return getCollectionOfValues(Arrays.copyOfRange(fields, 1, fields.length), values);
        }
        // Check if it's a map
        if (Map.class.isAssignableFrom(found.getClass()) && fields.length > 1) {
            return getValue(Arrays.copyOfRange(fields, 1, fields.length), (Map<?, ?>)found);
        }
        return found;
    }

    /**
     * Walk through the collection of values.
     * @param fields the fields that are fetched from inside the list of values
     * @param values the values to be searched.
     * @return a collection of the values found...
     */
    private static Collection<?> getCollectionOfValues(String[] fields, Collection<Map<?, ?>> values) {
        return values.stream().filter(map -> map != null && map.containsKey(fields[0]))
                .map(m -> getValue(fields, m)).collect(Collectors.toList());
    }
}
