package com.pungwe.db.engine.utils;

/**
 * Created by 917903 on 23/05/2016.
 */
public interface Constants {
    int BLOCK_SIZE = 4096; // always 4KB, never less, never more...
    int LOCK_WAIT = 200; // nanoseconds
    int LOCK_TIMEOUT = 60000; // 60 seconds
    int VALUE_SIZE = 16777216;
    int INDEX_ENTRY_SIZE = 1024; // this is to make the index keys more efficient...
    int INDEX_NODE_SIZE = 1048576; // 1MB
    int MIN_PAGE_SHIFT = 20; // 1MB
    int HASH_SEED = 0x9747b28c;
}
