package edu.uw.zookeeper.protocol.proto;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.List;
import java.util.TreeMap;

import org.apache.jute.OutputArchive;
import org.apache.jute.Record;

/**
 * Based on org.apache.jute.BinaryOutputArchive
 */
public class ByteBufOutputArchive extends ByteBufArchive implements OutputArchive {

    public ByteBufOutputArchive(ByteBuf buffer) {
        super(buffer);
    }
    
    @Override
    public void writeByte(byte b, String tag) throws IOException {
        buffer.writeByte(b);
    }

    @Override
    public void writeBool(boolean b, String tag) throws IOException {
        buffer.writeBoolean(b);
    }

    @Override
    public void writeInt(int i, String tag) throws IOException {
        buffer.writeInt(i);
    }

    @Override
    public void writeLong(long l, String tag) throws IOException {
        buffer.writeLong(l);
    }

    @Override
    public void writeFloat(float f, String tag) throws IOException {
        buffer.writeFloat(f);
    }

    @Override
    public void writeDouble(double d, String tag) throws IOException {
        buffer.writeDouble(d);
    }

    @Override
    public void writeString(String s, String tag) throws IOException {
        if (s == null) {
            writeInt(-1, Records.LEN_TAG);
            return;
        }
        byte[] bytes = s.getBytes(CHARSET);
        writeInt(bytes.length, Records.LEN_TAG);
        buffer.writeBytes(bytes);
    }

    @Override
    public void writeBuffer(byte[] bytes, String tag) throws IOException {
        if (bytes == null) {
            buffer.writeInt(-1);
            return;
        }
        buffer.writeInt(bytes.length);
        buffer.writeBytes(bytes);
    }

    @Override
    public void writeRecord(Record r, String tag) throws IOException {
        r.serialize(this, tag);
    }

    @Override
    public void startRecord(Record r, String tag) throws IOException {
    }

    @Override
    public void endRecord(Record r, String tag) throws IOException {
    }

    @Override
    public void startVector(List<?> v, String tag) throws IOException {
        if (v == null) {
            writeInt(-1, tag);
            return;
        }
        writeInt(v.size(), tag);
    }

    @Override
    public void endVector(List<?> v, String tag) throws IOException {
    }

    @Override
    public void startMap(TreeMap<?, ?> v, String tag) throws IOException {
        writeInt(v.size(), tag);
    }

    @Override
    public void endMap(TreeMap<?, ?> v, String tag) throws IOException {
    }
}
