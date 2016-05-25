package com.pungwe.db.core.io.serializers;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.io.*;

/**
 * Created by 917903 on 24/05/2016.
 */
public class LZ4Serializer<T> implements Serializer<T> {

    private final Serializer<T> serializer;

    public LZ4Serializer(Serializer<T> serializer) {
        this.serializer = serializer;
    }

    @Override
    public void serialize(DataOutput out, T value) throws IOException {
        LZ4Factory factory = LZ4Factory.fastestJavaInstance();
        LZ4Compressor compressor = factory.fastCompressor();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bytes);
        serializer.serialize(dos, value);
        byte[] result = compressor.compress(bytes.toByteArray());
        out.writeUTF("LZ4");
        out.writeInt(result.length);
        out.writeInt(bytes.size());
        out.write(result);
    }

    @Override
    public T deserialize(DataInput in) throws IOException {
        LZ4Factory factory = LZ4Factory.fastestJavaInstance();
        LZ4FastDecompressor decompressor = factory.fastDecompressor();
        String type = in.readUTF();
        if (!type.equalsIgnoreCase("LZ4")) {
            throw new IOException("Invalid LZ4 data stream.");
        }
        int len = in.readInt();
        int decLen = in.readInt();
        byte[] bytes = new byte[len];
        in.readFully(bytes);
        byte[] readBytes = decompressor.decompress(bytes, decLen);
        ByteArrayInputStream is = new ByteArrayInputStream(readBytes);
        DataInputStream dis = new DataInputStream(is);
        return serializer.deserialize(dis);
    }
}
