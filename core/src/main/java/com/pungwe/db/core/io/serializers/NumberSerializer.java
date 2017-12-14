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
 * Created by ian when 11/07/2016.
 */
public class NumberSerializer<T extends Number> implements Serializer<T> {

    private static final byte NUMBER = 'i';
    private static final byte BIG_NUMBER = 'I';
    private static final byte DECIMAL = 'd';
    private static final byte BIG_DECIMAL = 'D';

    private final Class<T> type;

    public NumberSerializer(Class<T> type) {
        this.type = type;
    }

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
        } else {
            out.writeByte(NUMBER);
            out.writeLong(value.longValue());
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public T deserialize(DataInput in) throws IOException {
        Number readValue = readValue(in);
        if (readValue == null) {
            return null;
        }
        if (Double.class.isAssignableFrom(type)) {
            return (T)new Double(readValue.doubleValue());
        } else if (Float.class.isAssignableFrom(type)) {
            return (T) new Float(readValue.floatValue());
        } else if (BigDecimal.class.isAssignableFrom(type)) {
            return (T)(BigDecimal.class.isAssignableFrom(readValue.getClass()) ? (BigDecimal)readValue :
                BigDecimal.valueOf(readValue.doubleValue()));
        } else if (Integer.class.isAssignableFrom(type)) {
            return (T)new Integer(readValue.intValue());
        } else if (BigInteger.class.isAssignableFrom(type)) {
            return (T)(BigInteger.class.isAssignableFrom(readValue.getClass()) ? (BigInteger)readValue :
                    BigInteger.valueOf(readValue.longValue()));
        } else if (Long.class.isAssignableFrom(type)) {
            return (T)new Long(readValue.longValue());
        }
        // Try cast to T...
        return (T)readValue;
    }

    private Number readValue(DataInput in) throws IOException {
        byte type = in.readByte();
        switch (type) {
            case NUMBER: {
                return new Long(in.readLong()); // return the long number
            }
            // FIXME: We can probably store more information about this...
            case BIG_NUMBER: {
                String value = in.readUTF();
                BigInteger bigInt = new BigInteger(value);
                return bigInt;
            }
            case DECIMAL: {
                return new Double(in.readDouble());
            }
            // FIXME: We can probably store more information about this...
            case BIG_DECIMAL: {
                String value = in.readUTF();
                BigDecimal bigDecimal = new BigDecimal(value);
                return bigDecimal;
            }
        }
        return null;
    }

    @Override
    public String getKey() {
        return type.getSimpleName();
    }
}
