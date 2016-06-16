package com.pungwe.db.core.io.volume;

import com.pungwe.db.core.utils.Constants;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by 917903 on 16/06/2016.
 */
public abstract class AbstractGrowableVolume implements Volume {

    private final ReentrantLock growLock = new ReentrantLock();

    protected void lock() throws IOException {
        long time = System.currentTimeMillis();
        while (!growLock.tryLock()) {
            if (System.currentTimeMillis() - time >= Constants.LOCK_TIMEOUT) {
                throw new IOException("Timed out whilst attempting to lock volme: "
                        + name());
            }
            // Sleep for LOCK_WAIT and try again
            try {
                Thread.sleep(0, Constants.LOCK_WAIT);
            } catch (InterruptedException ex) {
            }
        }
    }

    protected void unlock() {
        if (growLock.isHeldByCurrentThread()) {
            growLock.unlock();
        }
    }
}
