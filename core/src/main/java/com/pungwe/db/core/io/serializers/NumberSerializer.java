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
import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Created by ian on 11/07/2016.
 */
public class NumberSerializer<T extends Number> implements Serializer<T> {

    private static final byte NUMBER = 'i';
    private static final byte BIG_NUMBER = 'I';
    private static final byte DECIMAL = 'd';
    private static final byte BIG_DECIMAL = 'D';

    @Override
    public void serialize(DataOutput out, T value) throws IOException {
        if (Double.class.isAssignableFrom(value.getClass())) {
            out.writeByte(DECIMAL);
            out.writeDouble((Double) value);
        } else if (Float.class.isAssignableFrom(value.getClass())) {
            out.writeByte(DECIMAL);
            out.writeDouble((Float) value);
            // FIXME: We can probably store more information about this...
        } else if (BigDecimal.class.isAssignableFrom(value.getClass())) {
            out.writeByte(BIG_DECIMAL);
            out.writeUTF(value.toString());
            // FIXME: We can probably store more information about this...
        } else if (BigInteger.class.isAssignableFrom(value.getClass())) {
            out.writeByte(BIG_NUMBER);
            out.writeUTF(value.toString());
        } else if (Number.class.isAssignableFrom(value.getClass())) {
            out.writeByte(NUMBER);
            out.writeLong(value.longValue());
        }
    }

    @Override
    public T deserialize(DataInput in) throws IOException {
        byte type = in.readByte();
        switch (type) {
            case NUMBER: {
                return (T)new Long(in.readLong()); // return the long number
            }
            // FIXME: We can probably store more information about this...
            case BIG_NUMBER: {
                String value = in.readUTF();
                BigInteger bigInt = new BigInteger(value);
                return (T)bigInt;
            }
            case DECIMAL: {
                return (T)new Double(in.readDouble());
            }
            // FIXME: We can probably store more information about this...
            case BIG_DECIMAL: {
                String value = in.readUTF();
                BigDecimal bigDecimal = new BigDecimal(value);
                return (T)bigDecimal;
            }
        }
    }

    @Override
    public String getKey() {
        return "NUMBER";
    }
}
