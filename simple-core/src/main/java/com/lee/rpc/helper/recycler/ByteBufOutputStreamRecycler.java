package com.lee.rpc.helper.recycler;

import com.lee.rpc.util.stream.ByteBufOutputStream;
import io.netty.buffer.ByteBuf;
import io.netty.util.internal.ObjectPool;

/**
 * @author Administrator
 */
public class ByteBufOutputStreamRecycler {

    private static final ObjectPool<ByteBufOutputStream>
            RECYCLER = ObjectPool.newPool(ByteBufOutputStream::new);

    private ByteBufOutputStreamRecycler() {
    }

    public static ByteBufOutputStream newInstance(ByteBuf byteBuf) {
        ByteBufOutputStream byteBufOutputStream = RECYCLER.get();
        byteBufOutputStream.setByteBuf(byteBuf);
        return byteBufOutputStream;
    }
}
