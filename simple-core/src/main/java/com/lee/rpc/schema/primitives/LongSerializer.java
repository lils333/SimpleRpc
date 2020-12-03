package com.lee.rpc.schema.primitives;

import com.lee.rpc.RpcException;
import com.lee.rpc.schema.Serializer;
import com.lee.rpc.util.stream.ByteBufInputStream;
import com.lee.rpc.util.stream.ByteBufOutputStream;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class LongSerializer implements Serializer<Long> {

    public static final LongSerializer LONG_SERIALIZER = new LongSerializer();

    private LongSerializer() {
    }

    @Override
    public void serialize(Long obj, ByteBufOutputStream out) {
        try {
            out.writeLong(obj);
        } catch (IOException e) {
            throw new RpcException("Can not write int to output stream", e);
        }
    }

    @Override
    public Long deserialize(ByteBufInputStream in) {
        try {
            return in.readLong();
        } catch (IOException e) {
            throw new RpcException("Can not read short from input stream", e);
        }
    }
}
