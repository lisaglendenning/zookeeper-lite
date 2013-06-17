package edu.uw.zookeeper.protocol.proto;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

import org.apache.jute.Index;
import org.apache.jute.InputArchive;
import org.apache.jute.Record;

/**
 * Based on org.apache.jute.BinaryInputArchive
 */
public class ByteBufInputArchive extends ByteBufArchive implements InputArchive {

    public ByteBufInputArchive(ByteBuf buffer) {
        super(buffer);
    }
    
    @Override
    public byte readByte(String tag) throws IOException {
        return buffer.readByte();
    }

    @Override
    public boolean readBool(String tag) throws IOException {
        return buffer.readBoolean();
    }

    @Override
    public int readInt(String tag) throws IOException {
        return buffer.readInt();
    }

    @Override
    public long readLong(String tag) throws IOException {
        return buffer.readLong();
    }

    @Override
    public float readFloat(String tag) throws IOException {
        return buffer.readFloat();
    }

    @Override
    public double readDouble(String tag) throws IOException {
        return buffer.readDouble();
    }

    @Override
    public String readString(String tag) throws IOException {
        int len = buffer.readInt();
        if (len == -1) {
            return null;
        }
        byte b[] = new byte[len];
        buffer.readBytes(b);
        return new String(b, CHARSET);
    }

    @Override
    public byte[] readBuffer(String tag) throws IOException {
        int len = readInt(tag);
        if (len == -1) {
            return null;
        }
        byte b[] = new byte[len];
        buffer.readBytes(b);
        return b;
    }

    @Override
    public void readRecord(Record r, String tag) throws IOException {
        r.deserialize(this, tag);
    }

    @Override
    public void startRecord(String tag) throws IOException {
    }

    @Override
    public void endRecord(String tag) throws IOException {
    }

    @Override
    public Index startVector(String tag) throws IOException {
        int len = readInt(tag);
        if (len == -1) {
            return null;
        }
        return new BufferIndex(len);
    }

    @Override
    public void endVector(String tag) throws IOException {
    }

    @Override
    public Index startMap(String tag) throws IOException {
        return new BufferIndex(readInt(tag));
    }

    @Override
    public void endMap(String tag) throws IOException {
    }
    
    public static class BufferIndex implements Index {
        
        private int nleft;
        
        BufferIndex(int nelems) {
            this.nleft = nelems;
        }
        
        @Override
        public boolean done() {
            return (nleft <= 0);
        }

        @Override
        public void incr() {
            nleft--;
        }
    }
}
