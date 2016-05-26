package com.pungwe.db.core.io.serializers;

import org.joda.time.DateTime;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by ian on 25/05/2016.
 */
public class ObjectSerializerTest {

    @Test
    public void testSerializeDeserialize() throws Exception {

        List<String> list = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            list.add(i + "_string");
        }

        Map<String, Object> subObject = new HashMap<>();
        subObject.put("test", "value");

        Map<String, Object> map = new LinkedHashMap<>();
        map.put("null", null);
        map.put("boolean_true", true);
        map.put("boolean_false", false);
        map.put("string", "my string");
        map.put("integer", 1234);
        map.put("long", 12345l);
        map.put("big_number", BigInteger.valueOf(123456));
        map.put("decimal", 50.01d);
        map.put("float", 50.02f);
        map.put("big_decimal", BigDecimal.valueOf(50.03d));
        map.put("binary", new byte[] { 1, 2, 3, 4, 5 });
        map.put("array_list", list);
        map.put("array", new String[] {"1", "2", "3", "4", "5"});
        map.put("object", subObject);
        map.put("date", new Date());
        map.put("calendar", Calendar.getInstance());
        map.put("datetime", DateTime.now());

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(out);

        // Serialize the data from the map
        ObjectSerializer serializer = new ObjectSerializer();
        long start = System.nanoTime();
        serializer.serialize(dataOut, map);
        long end = System.nanoTime();

        System.out.println(String.format("Took: %f ms to serialize", (end - start) / 1000000d));

        // Deserialize the data from the map
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        DataInputStream dataIn = new DataInputStream(in);
        start = System.nanoTime();
        Map<String, Object> result = (Map<String, Object>)serializer.deserialize(dataIn);
        end = System.nanoTime();

        System.out.println(String.format("Took: %f ms to deserialize", (end - start) / 1000000d));

        for (Map.Entry<String, Object> entry : map.entrySet()) {
            assertTrue("Expected the key to be there: " + entry.getKey(), result.containsKey(entry.getKey()));
            // First things first, ensure that we get the value
            Object value = result.get(entry.getKey());
            if (entry.getValue() instanceof Integer) {
                assertEquals("Expected values to match", ((Integer)entry.getValue()).longValue(), value);
            } else if (entry.getValue() instanceof Float) {
                assertEquals("Expected values to match", ((Float) entry.getValue()).doubleValue(), value);
            } else if (entry.getValue() instanceof byte[]) {
                assertEquals("First byte should be 1", (byte) 1, ((byte[]) entry.getValue())[0]);
                assertEquals("Second byte should be 2", (byte) 2, ((byte[]) entry.getValue())[1]);
                assertEquals("Third byte should be 3", (byte) 3, ((byte[]) entry.getValue())[2]);
                assertEquals("Fourth byte should be 4", (byte) 4, ((byte[]) entry.getValue())[3]);
                assertEquals("Fith byte should be 5", (byte) 5, ((byte[]) entry.getValue())[4]);
            } else if (entry.getValue() instanceof String[]) {
                assertEquals("First entry should be 1", "1", ((String[]) entry.getValue())[0]);
                assertEquals("Second entry should be 2", "2", ((String[]) entry.getValue())[1]);
                assertEquals("Third entry should be 3", "3", ((String[]) entry.getValue())[2]);
                assertEquals("Fourth entry should be 4", "4", ((String[]) entry.getValue())[3]);
                assertEquals("Fith entry should be 5", "5", ((String[]) entry.getValue())[4]);
            } else if (entry.getValue() instanceof Date) {
                Calendar c = Calendar.getInstance();
                c.setTime((Date) entry.getValue());
                DateTime dateTime = new DateTime(c);
                assertEquals("Time should match", 0, dateTime.compareTo((DateTime) value));
            } else if (entry.getValue() instanceof Calendar) {
                DateTime dateTime = new DateTime((Calendar)entry.getValue());
                assertEquals("Time should match", 0, dateTime.compareTo((DateTime) value));
            } else {
                assertEquals("Expected values to match", entry.getValue(), value);
            }
        }
    }
}
