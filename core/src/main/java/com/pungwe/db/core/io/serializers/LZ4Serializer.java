package com.pungwe.db.core.io.serializers;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.io.*;

/**
 * Created by 917903 on 24/05/2016.
 */
@Deprecated
public class LZ4Serializer implements Serializer {

    private final Serializer serializer;

    public LZ4Serializer(Serializer serializer) {
        this.serializer = serializer;
    }

    @Override
    public void serialize(DataOutput out, Object value) throws IOException {
        LZ4Factory factory = LZ4Factory.fastestJavaInstance();
        LZ4Compressor compressor = factory.fastCompressor();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bytes);
        serializer.serialize(dos, value);
        byte[] result = compressor.compress(bytes.toByteArray());
        out.writeUTF(getKey());
        out.writeInt(result.length);
        out.writeInt(bytes.size());
        out.write(result);
    }

    @Override
    public Object deserialize(DataInput in) throws IOException {
        LZ4Factory factory = LZ4Factory.fastestJavaInstance();
        LZ4FastDecompressor decompressor = factory.fastDecompressor();
        String type = in.readUTF();
        if (!type.equalsIgnoreCase(getKey())) {
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

    @Override
    public String getKey() {
        return "LZ4:" + serializer.getKey();
    }
}
