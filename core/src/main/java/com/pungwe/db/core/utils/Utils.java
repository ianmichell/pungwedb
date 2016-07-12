package com.pungwe.db.core.utils;

import net.jpountz.xxhash.StreamingXXHash64;
import net.jpountz.xxhash.XXHashFactory;

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

}
