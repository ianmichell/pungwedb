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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ian on 10/07/2016.
 */
public class KryoSerializer<E> implements Serializer<E> {

    private final Class<E> type;

    public KryoSerializer(Class<E> type) {
        this.type = type;
    }

    @Override
    public void serialize(DataOutput out, E value) throws IOException {
    }

    @Override
    public E deserialize(DataInput in) throws IOException {
        return null;
    }

    @Override
    public String getKey() {
        return null;
    }
}
