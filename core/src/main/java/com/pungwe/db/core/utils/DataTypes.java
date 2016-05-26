package com.pungwe.db.core.utils;

/**
 * Created by ian on 25/05/2016.
 */
public enum DataTypes {

    NULL((byte)'N'), BOOLEAN((byte)'b'), NUMBER((byte)'I'), BINARY((byte)'B'), DECIMAL((byte)'D'),
    STRING((byte)'S'), TIMESTAMP((byte)'Z'), ARRAY((byte)'A'), OBJECT((byte)'O');

    private byte type;

    private DataTypes(byte type) {
        this.type = type;
    }

    public byte getType() {
        return this.type;
    }

    public static DataTypes fromType(byte type) {
        switch (type) {
            case 'N':
                return NULL;
            case 'b':
                return BOOLEAN;
            case 'I':
                return NUMBER;
            case 'B':
                return BINARY;
            case 'D':
                return DECIMAL;
            case 'S':
                return STRING;
            case 'Z':
                return TIMESTAMP;
            case 'A':
                return ARRAY;
            case 'O':
                return OBJECT;
        }
        throw new IllegalArgumentException("Could not identify data type");
    }
}
