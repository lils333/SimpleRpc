package com.lee.rpc.helper.recycler;

import com.lee.rpc.util.stream.ByteBufInputStream;
import io.netty.buffer.ByteBuf;
import io.netty.util.internal.ObjectPool;

/**
 * @author Administrator
 */
public class ByteBufInputStreamRecycler {

    private static final ObjectPool<ByteBufInputStream>
            RECYCLER = ObjectPool.newPool(ByteBufInputStream::new);

    private ByteBufInputStreamRecycler() {
    }

    public static ByteBufInputStream newInstance(ByteBuf byteBuf) {
        ByteBufInputStream byteBuffInputStream = RECYCLER.get();
        byteBuffInputStream.setByteBuf(byteBuf);
        return byteBuffInputStream;
    }
}
