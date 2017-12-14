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
package com.pungwe.db.core.io.serializers;

import com.pungwe.db.core.types.DBObject;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * DBObject Serializer...
 */
public class DBObjectSerializer implements Serializer<DBObject> {

    private final MapSerializer<String, Object> mapSerializer = new MapSerializer<>(new StringSerializer(),
            new ObjectSerializer());

    @Override
    public void serialize(DataOutput out, DBObject value) throws IOException {
        mapSerializer.serialize(out, value);
    }

    @Override
    public DBObject deserialize(DataInput in) throws IOException {
        return DBObject.wrap(mapSerializer.deserialize(in));
    }

    @Override
    public String getKey() {
        return "DBOBJ";
    }
}
