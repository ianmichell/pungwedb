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
 * Created by ian when 25/05/2016.
 */
public class ObjectSerializer implements Serializer<Object> {

    private static final byte NULL = 'N';
    private static final byte STRING = 'S';
    private static final byte NUMBER = 'i';
    private static final byte BIG_NUMBER = 'I';
    private static final byte DECIMAL = 'd';
    private static final byte BIG_DECIMAL = 'D';
    private static final byte BOOLEAN = 'B';
    private static final byte BINARY = 'b';
    private static final byte TIMESTAMP = 'T';
    private static final byte MAP = 'M';
    private static final byte ARRAY = 'A';
    private static final byte UUID = 'U';

    private static final byte KEY = 'K';
    private static final byte VALUE = 'V';
    private static final byte ENTRY = 'E';

    @Override
    public void serialize(DataOutput out, Object value) throws IOException {
        writeValue(out, value);
    }

    @SuppressWarnings("unchecked")
    private void writeValue(DataOutput out, Object value) throws IOException {
        if (value == null) {
            out.writeByte(NULL);
            return;
        }

        if (byte[].class.isAssignableFrom(value.getClass())) {
            // Binary!
            out.writeByte(BINARY);
            new ByteSerializer().serialize(out, (byte[])value);
        } else if (value.getClass().isArray()) {
            out.writeByte(ARRAY);
            out.writeInt(Array.getLength(value));
            for (int i = 0; i < Array.getLength(value); i++) {
                writeEntry(out, Array.get(value, i));
            }
        } else if (String.class.isAssignableFrom(value.getClass())) {
            out.writeByte(STRING);
            new StringSerializer().serialize(out, (String)value);
        } else if (Boolean.class.isAssignableFrom(value.getClass())) {
            out.writeByte(BOOLEAN);
            out.writeByte(((Boolean) value) ? 1 : 0);
        } else if (Double.class.isAssignableFrom(value.getClass())) {
            out.writeByte(DECIMAL);
            new NumberSerializer<>(Double.class).serialize(out, (Double)value);
        } else if (Float.class.isAssignableFrom(value.getClass())) {
            out.writeByte(DECIMAL);
            new NumberSerializer<>(Float.class).serialize(out, (Float)value);
            // FIXME: We can probably store more information about this...
        } else if (BigDecimal.class.isAssignableFrom(value.getClass())) {
            out.writeByte(BIG_DECIMAL);
            new NumberSerializer<>(BigDecimal.class).serialize(out, (BigDecimal) value);
            // FIXME: We can probably store more information about this...
        } else if (BigInteger.class.isAssignableFrom(value.getClass())) {
            out.writeByte(BIG_NUMBER);
            new NumberSerializer<>(BigInteger.class).serialize(out, (BigInteger) value);
        } else if (Number.class.isAssignableFrom(value.getClass())) {
            out.writeByte(NUMBER);
            new NumberSerializer<>(Long.class).serialize(out, ((Number)value).longValue());
        } else if (Map.class.isAssignableFrom(value.getClass())) {
            out.writeByte(MAP);
            new MapSerializer<>(this, this).serialize(out, (Map<Object, Object>)value);
        } else if (Collection.class.isAssignableFrom(value.getClass())) {
            out.writeByte(ARRAY);
            out.writeInt(((Collection)value).size());
            Iterator<Object> it = ((Iterable<Object>) value).iterator();
            while (it.hasNext()) {
                writeEntry(out, it.next());
            }
        } else if (Date.class.isAssignableFrom(value.getClass())) {
            writeDate(out, (Date) value);
        } else if (Calendar.class.isAssignableFrom(value.getClass())) {
            writeDate(out, (Calendar) value);
        } else if (DateTime.class.isAssignableFrom(value.getClass())) {
            writeDate(out, (DateTime) value);
        } else if (UUID.class.isAssignableFrom(value.getClass())) {
            out.writeByte(UUID);
            new UUIDSerializer().serialize(out, (UUID)value);
        } else {
            throw new IllegalArgumentException(String.format("%s call not a supported data type",
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
        return readValue(in);
    }

    @SuppressWarnings("unchecked")
    private Object readValue(DataInput in) throws IOException {
        // Fetch the first byte!
        byte type = in.readByte();
        switch (type) {
            case NULL: {
                return null;
            }
            case STRING: {
                return new StringSerializer().deserialize(in);
            }
            case NUMBER: {
                // FIXME: Use a registry
                return new NumberSerializer<>(Long.class).deserialize(in);
            }
            // FIXME: We can probably store more information about this...
            case BIG_NUMBER: {
               return new NumberSerializer<>(BigInteger.class).deserialize(in);
            }
            case DECIMAL: {
                return new NumberSerializer<>(Double.class).deserialize(in);
            }
            // FIXME: We can probably store more information about this...
            case BIG_DECIMAL: {
                return new NumberSerializer<>(BigDecimal.class).deserialize(in);
            }
            case BINARY: {
                return new ByteSerializer().deserialize(in);
            }
            case BOOLEAN: {
                byte b = in.readByte();
                return b == 1;
            }
            case TIMESTAMP: {
                return readTimestamp(in);
            }
            case MAP: {
                return new MapSerializer<>(this, this).deserialize(in);
            }
            case ARRAY: {
                int size = in.readInt(); // get number of entries
                List<Object>  array = new ArrayList<>();
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
