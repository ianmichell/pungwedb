/*
 * Copyright (C) 2016 Ian Michell.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pungwe.db.engine.collections.btree;

import com.pungwe.db.core.io.serializers.ObjectSerializer;
import com.pungwe.db.core.io.serializers.Serializer;
import com.pungwe.db.engine.io.BasicRecordFile;
import com.pungwe.db.engine.io.RecordFile;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by ian on 11/07/2016.
 */
public class ImmutableBTreeMapTest {

    @Test
    public void writeAndLoad() throws IOException {
        // Add a million records to map
        BTreeMap<Long, Long> map = new BTreeMap<>(Long::compareTo, 10);
        long start = System.nanoTime();
        for (long i = 0; i < 10000; i++) {
            assertNotNull(map.put(i, i));
        }
        long end = System.nanoTime();
        System.out.println(String.format("Took: %f ms to put", (end - start) / 1000000d));

        File tmp = File.createTempFile("immutable", ".db");
        try {
            RecordFile<AbstractBTreeMap.Node<Long, ?>> recordFile = new BasicRecordFile<>(tmp,
                    ImmutableBTree.serializer(new ObjectSerializer(), new ObjectSerializer()));
            start = System.nanoTime();
            ImmutableBTree<Long, Long> immutableMap = ImmutableBTree.write(recordFile, "test", map);
            end = System.nanoTime();
            System.out.println(String.format("Took: %f ms to write and load immutable tree", (end - start) / 1000000d));

            // Ensure we have every record
            for (long i = 0; i < 10000; i++) {
                assertEquals(new Long(i), immutableMap.get(i));
            }
        } finally {
            tmp.delete();
        }
    }
}
