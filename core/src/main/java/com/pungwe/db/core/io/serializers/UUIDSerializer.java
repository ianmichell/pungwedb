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

import com.pungwe.db.core.utils.UUIDGen;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

/**
 * Created by ian when 16/07/2016.
 */
public class UUIDSerializer implements Serializer<UUID> {

    @Override
    public void serialize(DataOutput out, UUID value) throws IOException {
        out.write(UUIDGen.asByteArray(value));
    }

    @Override
    public UUID deserialize(DataInput in) throws IOException {
        byte[] bytes = new byte[16];
        in.readFully(bytes);
        return UUIDGen.getUUID(bytes);
    }

    @Override
    public String getKey() {
        return "UUID";
    }
}
