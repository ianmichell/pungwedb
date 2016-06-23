package com.pungwe.db.core.io.serializers;

import com.pungwe.db.core.utils.UUIDGen;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

/**
 * Created by ian on 25/05/2016.
 */
public class ObjectSerializer implements Serializer {

    private static final byte NULL = 'N';
    private static final byte STRING = 'S';
    private static final byte NUMBER = 'i';
    private static final byte BIG_NUMBER = 'I';
    private static final byte DECIMAL = 'd';
    private static final byte BIG_DECIMAL = 'D';
    private static final byte BOOLEAN = 'B';
    private static final byte BINARY = 'b';
    private static final byte TIMESTAMP = 'T';
    private static final byte OBJECT = 'O';
    private static final byte ARRAY = 'A';
    private static final byte UUID = 'U';

    private static final byte KEY = 'K';
    private static final byte VALUE = 'V';
    private static final byte ENTRY = 'E';

    @Override
    public void serialize(DataOutput out, Object value) throws IOException {
        out.writeUTF(getKey());
        writeValue(out, value);
    }

    private void writeValue(DataOutput out, Object value) throws IOException {
        if (value == null) {
            out.writeByte(NULL);
            return;
        }

        if (byte[].class.isAssignableFrom(value.getClass())) {
            // Binary!
            out.writeByte(BINARY);
            out.writeInt(((byte[])value).length);
            out.write((byte[])value);
        } else if (value.getClass().isArray()) {
            out.writeByte(ARRAY);
            out.writeInt(Array.getLength(value));
            for (int i = 0; i < Array.getLength(value); i++) {
                writeEntry(out, Array.get(value, i));
            }
        } else if (String.class.isAssignableFrom(value.getClass())) {
            out.writeByte(STRING);
            out.writeUTF((String) value);
        } else if (Boolean.class.isAssignableFrom(value.getClass())) {
            out.writeByte(BOOLEAN);
            out.writeByte(((Boolean) value) ? 1 : 0);
        } else if (Double.class.isAssignableFrom(value.getClass())) {
            out.writeByte(DECIMAL);
            out.writeDouble((Double) value);
        } else if (Float.class.isAssignableFrom(value.getClass())) {
            out.writeByte(DECIMAL);
            out.writeDouble((Float) value);
            // FIXME: We can probably store more information about this...
        } else if (BigDecimal.class.isAssignableFrom(value.getClass())) {
            out.writeByte(BIG_DECIMAL);
            out.writeUTF(((BigDecimal) value).toString());
            // FIXME: We can probably store more information about this...
        } else if (BigInteger.class.isAssignableFrom(value.getClass())) {
            out.writeByte(BIG_NUMBER);
            out.writeUTF(((BigInteger) value).toString());
        } else if (Number.class.isAssignableFrom(value.getClass())) {
            out.writeByte(NUMBER);
            out.writeLong(((Number) value).longValue());
        } else if (Map.class.isAssignableFrom(value.getClass())) {
            out.writeByte(OBJECT);
            out.writeInt(((Map)value).size());
            for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) value).entrySet()) {
                writeEntry(out, entry);
            }
        } else if (Collection.class.isAssignableFrom(value.getClass())) {
            out.writeByte(ARRAY);
            out.writeInt(((Collection)value).size());
            Iterator<Object> it = ((Iterable<Object>) value).iterator();
            while (it.hasNext()) {
                writeEntry(out, it.next());
            }
        } else if (Map.Entry.class.isAssignableFrom(value.getClass())) {
            out.writeByte(KEY);
            writeValue(out, ((Map.Entry) value).getKey());
            out.writeByte(VALUE);
            writeValue(out, ((Map.Entry) value).getValue());
        } else if (Date.class.isAssignableFrom(value.getClass())) {
            // We want to write an appropriate date, so for now set this to null and
            // we will create a special date object to store all the correct and relevant time values.
            // It will probably need to use joda... But it will certainly be UTC based time stamps
            writeDate(out, (Date) value);
        } else if (Calendar.class.isAssignableFrom(value.getClass())) {
            writeDate(out, (Calendar) value);
        } else if (DateTime.class.isAssignableFrom(value.getClass())) {
            writeDate(out, (DateTime) value);
        } else if (UUID.class.isAssignableFrom(value.getClass())) {
            out.writeByte(UUID);
            out.write(UUIDGen.asByteArray((UUID)value));
        } else {
            throw new IllegalArgumentException(String.format("%s is not a supported data type",
                    value.getClass().getName()));
        }
    }

    private void writeDate(DataOutput out, Date date) throws IOException {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        writeDate(out, c);
    }

    private void writeDate(DataOutput out, Calendar calendar) throws IOException {
        DateTime date = new DateTime(calendar);
        writeDate(out, date);
    }

    private void writeDate(DataOutput out, DateTime date) throws IOException {
        DateTimeFormatter fmt = ISODateTimeFormat.dateTime();
        out.writeByte(TIMESTAMP);
        out.writeUTF(fmt.print(date));
    }

    private void writeEntry(DataOutput out, Object entry) throws IOException {
        out.writeByte(ENTRY);
        writeValue(out, entry);
    }

    @Override
    public Object deserialize(DataInput in) throws IOException {
        String type = in.readUTF();
        if (!type.equalsIgnoreCase(getKey())) {
            throw new IOException("Invalid LZ4 data stream.");
        }
        return readValue(in);
    }

    private Object readValue(DataInput in) throws IOException {
        // Fetch the first byte!
        byte type = in.readByte();
        switch (type) {
            case NULL: {
                return null;
            }
            case STRING: {
                return in.readUTF(); // return UTF string
            }
            case NUMBER: {
                return in.readLong(); // return the long number
            }
            // FIXME: We can probably store more information about this...
            case BIG_NUMBER: {
                String value = in.readUTF();
                BigInteger bigInt = new BigInteger(value);
                return bigInt;
            }
            case DECIMAL: {
                return in.readDouble();
            }
            // FIXME: We can probably store more information about this...
            case BIG_DECIMAL: {
                String value = in.readUTF();
                BigDecimal bigDecimal = new BigDecimal(value);
                return bigDecimal;
            }
            case BINARY: {
                int size = in.readInt();
                byte[] b = new byte[size];
                in.readFully(b);
                return b;
            }
            case BOOLEAN: {
                byte b = in.readByte();
                return b == 1;
            }
            case TIMESTAMP: {
                return readTimestamp(in);
            }
            case OBJECT: {
                int size = in.readInt(); // get number of entries
                Map<Object, Object> map = new LinkedHashMap<>();
                for (int i = 0; i < size; i++) {
                    byte entryType = in.readByte();
                    if (entryType != ENTRY) {
                        throw new IOException("Expected to find an ENTRY (" + ENTRY + ") but was: " + entryType);
                    }
                    // Extract map key
                    byte keyType = in.readByte();
                    if (keyType != KEY) {
                        throw new IOException("Expected type to be that of KEY");
                    }
                    Object key = readValue(in);
                    // Extract key value
                    byte valueType = in.readByte();
                    if (valueType != VALUE) {
                        throw new IOException("Expected to find data type of VALUE");
                    }
                    Object value = readValue(in);
                    map.put(key, value);
                }
                return map;
            }
            case ARRAY: {
                int size = in.readInt(); // get number of entries
                LinkedList<Object>  array = new LinkedList<>();
                for (int i = 0; i < size; i++) {
                    byte entryType = in.readByte();
                    if (entryType != ENTRY) {
                        throw new IOException("Expected an ENTRY('E')");
                    }
                    array.add(readValue(in));
                }
                return array;
            }
            case UUID: {
                byte[] b = new byte[16];
                in.readFully(b);
                return UUIDGen.getUUID(b);
            }
        }
        throw new IOException("Could not find a recognized object");
    }

    private DateTime readTimestamp(DataInput input) throws IOException {
        String dateString = input.readUTF();
        DateTimeFormatter fmt = ISODateTimeFormat.dateTime();
        return fmt.parseDateTime(dateString);
    }

    @Override
    public String getKey() {
        return "OBJ";
    }
}
