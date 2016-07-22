package com.pungwe.db.core.io.serializers;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by ian when 25/05/2016.
 */
public class LZ4SerializerTest {

    @SuppressWarnings("unchecked")
    @Test
    public void testLZ4Serializer() throws Exception {

        ObjectSerializer serializer = new ObjectSerializer();
        LZ4Serializer lz4Serializer = new LZ4Serializer(serializer);

        Map<String, String> testMap = new LinkedHashMap<>();
        testMap.put("test", "this is a nice long test string, when we would like to compress and ensure matches " +
                "exatcly when we decompress it!");

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
        lz4Serializer.serialize(dataOutputStream, testMap);
        // Deserialize
        ByteArrayInputStream in = new ByteArrayInputStream(outputStream.toByteArray());
        DataInputStream dataIn = new DataInputStream(in);
        Map<String, String> result = (Map<String, String>)lz4Serializer.deserialize(dataIn);

        assertEquals("Test key should be equal in both maps", testMap.get("test"), result.get("test"));
    }
}
