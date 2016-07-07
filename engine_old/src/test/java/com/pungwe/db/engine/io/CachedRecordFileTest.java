package com.pungwe.db.engine.io;

import com.pungwe.db.core.io.serializers.ObjectSerializer;
import com.pungwe.db.core.io.serializers.StringSerializer;
import org.junit.Test;

import java.io.File;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;

/**
 * Created by 917903 on 06/07/2016.
 */
public class CachedRecordFileTest {

    @Test
    public void testAppendRead() throws Exception {
        File file = File.createTempFile("log_", ".db");
        CachedRecordFile<String> recordFile = new CachedRecordFile<>(file, new StringSerializer(), 10000);
        for (int i = 1; i <= 1000000; i++) {
            recordFile.writer().append("My String: " + i);
        }
        recordFile.writer().sync();
        int i = 0;
        Iterator<String> it = recordFile.reader();
        while (it.hasNext()) {
            try {
                assertEquals("My String: " + ++i, it.next());
            } catch (Exception ex) {
                System.out.println("Failed at record: " + i);
                throw ex;
            }
        }
        assertEquals(1000000, i);
    }
}
