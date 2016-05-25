package com.pungwe.db.core.utils;

/**
 * Created by 917903 on 23/05/2016.
 */
public interface Constants {
    int BLOCK_SIZE = 4096; // always 4KB, never less, never more...
    int LOCK_WAIT = 200; // nanoseconds
    int LOCK_TIMEOUT = 60000; // 60 seconds
}
