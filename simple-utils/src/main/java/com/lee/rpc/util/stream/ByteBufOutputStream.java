package com.lee.rpc.util.stream;

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;
import io.netty.util.internal.ObjectPool;
import io.netty.util.internal.ObjectUtil;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * @author Administrator
 */
public class ByteBufOutputStream extends OutputStream implements DataOutput {

    private final ObjectPool.Handle<ByteBufOutputStream> handle;
    private ByteBuf buffer;
    private int startIndex;
    private DataOutputStream utf8out = new DataOutputStream(this);

    /**
     * Creates a new stream which writes data to the specified {@code buffer}.
     */
    public ByteBufOutputStream(ByteBuf buffer, ObjectPool.Handle<ByteBufOutputStream> handle) {
        this.buffer = ObjectUtil.checkNotNull(buffer, "buffer");
        startIndex = buffer.writerIndex();
        this.handle = handle;
    }

    public ByteBufOutputStream(ObjectPool.Handle<ByteBufOutputStream> handle) {
        this.handle = handle;
    }

    /**
     * Returns the number of written bytes by this stream so far.
     */
    public int writtenBytes() {
        return buffer.writerIndex() - startIndex;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (len == 0) {
            return;
        }

        buffer.writeBytes(b, off, len);
    }

    @Override
    public void write(byte[] b) throws IOException {
        buffer.writeBytes(b);
    }

    @Override
    public void write(int b) throws IOException {
        buffer.writeByte(b);
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        buffer.writeBoolean(v);
    }

    @Override
    public void writeByte(int v) throws IOException {
        buffer.writeByte(v);
    }

    @Override
    public void writeBytes(String s) throws IOException {
        buffer.writeCharSequence(s, CharsetUtil.US_ASCII);
    }

    @Override
    public void writeChar(int v) throws IOException {
        buffer.writeChar(v);
    }

    @Override
    public void writeChars(String s) throws IOException {
        int len = s.length();
        for (int i = 0; i < len; i++) {
            buffer.writeChar(s.charAt(i));
        }
    }

    @Override
    public void writeDouble(double v) throws IOException {
        buffer.writeDouble(v);
    }

    @Override
    public void writeFloat(float v) throws IOException {
        buffer.writeFloat(v);
    }

    @Override
    public void writeInt(int v) throws IOException {
        buffer.writeInt(v);
    }

    @Override
    public void writeLong(long v) throws IOException {
        buffer.writeLong(v);
    }

    @Override
    public void writeShort(int v) throws IOException {
        buffer.writeShort((short) v);
    }

    @Override
    public void writeUTF(String s) throws IOException {
        utf8out.writeUTF(s);
    }

    /**
     * Returns the buffer where this stream is writing data.
     */
    public ByteBuf buffer() {
        return buffer;
    }

    public void setByteBuf(ByteBuf byteBuf) {
        this.buffer = ObjectUtil.checkNotNull(byteBuf, "buffer");
        startIndex = buffer.writerIndex();
    }

    public void recycle() {
        this.buffer = null;
        this.startIndex = 0;
        handle.recycle(this);
    }

    @Override
    public void close() throws IOException {
        super.close();
        recycle();
    }
}
