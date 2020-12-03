package com.lee.rpc.util.stream;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCounted;
import io.netty.util.internal.ObjectPool;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.StringUtil;

import java.io.*;

/**
 * 从Netty那边copy过来，主要是为了支持重用, 使用Netty的Recycler 对象池功能
 *
 * @author Administrator
 */
public class ByteBufInputStream extends InputStream implements DataInput {

    private final ObjectPool.Handle<ByteBufInputStream> handle;

    private ByteBuf buffer;
    private int startIndex;
    private int endIndex;
    private boolean closed;
    /**
     * To preserve backwards compatibility (which didn't transfer ownership) we support a conditional flag which
     * indicates if {@link #buffer} should be released when this {@link InputStream} is closed.
     * However in future releases ownership should always be transferred and callers of this class should call
     * {@link ReferenceCounted#retain()} if necessary.
     */
    private boolean releaseOnClose;

    public ByteBufInputStream(ObjectPool.Handle<ByteBufInputStream> handle) {
        this.handle = handle;
    }

    /**
     * Creates a new stream which reads data from the specified {@code buffer}
     * starting at the current {@code readerIndex} and ending at the current
     * {@code writerIndex}.
     *
     * @param buffer The buffer which provides the content for this {@link InputStream}.
     */
    public ByteBufInputStream(ByteBuf buffer) {
        this(buffer, buffer.readableBytes(), null);
    }

    /**
     * Creates a new stream which reads data from the specified {@code buffer}
     * starting at the current {@code readerIndex} and ending at
     * {@code readerIndex + length}.
     *
     * @param buffer The buffer which provides the content for this {@link InputStream}.
     * @param length The length of the buffer to use for this {@link InputStream}.
     * @throws IndexOutOfBoundsException if {@code readerIndex + length} is greater than
     *                                   {@code writerIndex}
     */
    public ByteBufInputStream(ByteBuf buffer, int length, ObjectPool.Handle<ByteBufInputStream> handle) {
        this(buffer, length, false, handle);
    }

    /**
     * Creates a new stream which reads data from the specified {@code buffer}
     * starting at the current {@code readerIndex} and ending at the current
     * {@code writerIndex}.
     *
     * @param buffer         The buffer which provides the content for this {@link InputStream}.
     * @param releaseOnClose {@code true} means that when {@link #close()} is called then {@link ByteBuf#release()} will
     *                       be called on {@code buffer}.
     */
    public ByteBufInputStream(ByteBuf buffer, boolean releaseOnClose, ObjectPool.Handle<ByteBufInputStream> handle) {
        this(buffer, buffer.readableBytes(), releaseOnClose, handle);
    }

    /**
     * Creates a new stream which reads data from the specified {@code buffer}
     * starting at the current {@code readerIndex} and ending at
     * {@code readerIndex + length}.
     *
     * @param buffer         The buffer which provides the content for this {@link InputStream}.
     * @param length         The length of the buffer to use for this {@link InputStream}.
     * @param releaseOnClose {@code true} means that when {@link #close()} is called then {@link ByteBuf#release()} will
     *                       be called on {@code buffer}.
     * @throws IndexOutOfBoundsException if {@code readerIndex + length} is greater than
     *                                   {@code writerIndex}
     */
    public ByteBufInputStream(ByteBuf buffer, int length, boolean releaseOnClose, ObjectPool.Handle<ByteBufInputStream> handle) {
        ObjectUtil.checkNotNull(buffer, "buffer");
        if (length < 0) {
            if (releaseOnClose) {
                buffer.release();
            }
            throw new IllegalArgumentException("length: " + length);
        }
        if (length > buffer.readableBytes()) {
            if (releaseOnClose) {
                buffer.release();
            }
            throw new IndexOutOfBoundsException("Too many bytes to be read - Needs "
                    + length + ", maximum is " + buffer.readableBytes());
        }

        this.releaseOnClose = releaseOnClose;
        this.buffer = buffer;
        startIndex = buffer.readerIndex();
        endIndex = startIndex + length;
        buffer.markReaderIndex();
        this.handle = handle;
    }

    /**
     * Returns the number of read bytes by this stream so far.
     */
    public int readBytes() {
        return buffer.readerIndex() - startIndex;
    }

    @Override
    public void close() throws IOException {
        try {
            super.close();
        } finally {
            // The Closable interface says "If the stream is already closed then invoking this method has no effect."
            if (releaseOnClose && !closed) {
                closed = true;
                buffer.release();
            }

            if (handle != null) {
                recycle();
            }
        }
    }

    @Override
    public int available() throws IOException {
        return endIndex - buffer.readerIndex();
    }

    @Override
    public void mark(int readlimit) {
        buffer.markReaderIndex();
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public int read() throws IOException {
        int available = available();
        if (available == 0) {
            return -1;
        }
        return buffer.readByte() & 0xff;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int available = available();
        if (available == 0) {
            return -1;
        }

        len = Math.min(available, len);
        buffer.readBytes(b, off, len);
        return len;
    }

    @Override
    public void reset() throws IOException {
        buffer.resetReaderIndex();
    }

    @Override
    public long skip(long n) throws IOException {
        if (n > Integer.MAX_VALUE) {
            return skipBytes(Integer.MAX_VALUE);
        } else {
            return skipBytes((int) n);
        }
    }

    @Override
    public boolean readBoolean() throws IOException {
        checkAvailable(1);
        return read() != 0;
    }

    @Override
    public byte readByte() throws IOException {
        int available = available();
        if (available == 0) {
            throw new EOFException();
        }
        return buffer.readByte();
    }

    @Override
    public char readChar() throws IOException {
        return (char) readShort();
    }

    @Override
    public double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

    @Override
    public float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        readFully(b, 0, b.length);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        checkAvailable(len);
        buffer.readBytes(b, off, len);
    }

    @Override
    public int readInt() throws IOException {
        checkAvailable(4);
        return buffer.readInt();
    }

    private StringBuilder lineBuf;

    @Override
    public String readLine() throws IOException {
        int available = available();
        if (available == 0) {
            return null;
        }

        if (lineBuf != null) {
            lineBuf.setLength(0);
        }

        loop:
        do {
            int c = buffer.readUnsignedByte();
            --available;
            switch (c) {
                case '\n':
                    break loop;

                case '\r':
                    if (available > 0 && (char) buffer.getUnsignedByte(buffer.readerIndex()) == '\n') {
                        buffer.skipBytes(1);
                        --available;
                    }
                    break loop;

                default:
                    if (lineBuf == null) {
                        lineBuf = new StringBuilder();
                    }
                    lineBuf.append((char) c);
            }
        } while (available > 0);

        return lineBuf != null && lineBuf.length() > 0 ? lineBuf.toString() : StringUtil.EMPTY_STRING;
    }

    @Override
    public long readLong() throws IOException {
        checkAvailable(8);
        return buffer.readLong();
    }

    @Override
    public short readShort() throws IOException {
        checkAvailable(2);
        return buffer.readShort();
    }

    @Override
    public String readUTF() throws IOException {
        return DataInputStream.readUTF(this);
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return readByte() & 0xff;
    }

    @Override
    public int readUnsignedShort() throws IOException {
        return readShort() & 0xffff;
    }

    @Override
    public int skipBytes(int n) throws IOException {
        int nBytes = Math.min(available(), n);
        buffer.skipBytes(nBytes);
        return nBytes;
    }

    private void checkAvailable(int fieldSize) throws IOException {
        if (fieldSize < 0) {
            throw new IndexOutOfBoundsException("fieldSize cannot be a negative number");
        }
        if (fieldSize > available()) {
            throw new EOFException("fieldSize is too long! Length is " + fieldSize
                    + ", but maximum is " + available());
        }
    }

    public void setByteBuf(ByteBuf buffer) {
        ObjectUtil.checkNotNull(buffer, "buffer");
        int length = buffer.readableBytes();
        if (length < 0) {
            if (releaseOnClose) {
                buffer.release();
            }
            throw new IllegalArgumentException("length: " + length);
        }
        if (length > buffer.readableBytes()) {
            if (releaseOnClose) {
                buffer.release();
            }
            throw new IndexOutOfBoundsException("Too many bytes to be read - Needs "
                    + length + ", maximum is " + buffer.readableBytes());
        }
        this.releaseOnClose = false;
        this.buffer = buffer;
        startIndex = buffer.readerIndex();
        endIndex = startIndex + length;
        buffer.markReaderIndex();
    }

    public void recycle() {
        this.buffer = null;
        this.startIndex = 0;
        this.endIndex = 0;
        handle.recycle(this);
    }
}
