package com.pungwe.db.engine.io;

import java.io.IOException;

/**
 * Created by 917903 on 06/07/2016.
 */
public interface Syncable {

    void sync() throws IOException;

}
