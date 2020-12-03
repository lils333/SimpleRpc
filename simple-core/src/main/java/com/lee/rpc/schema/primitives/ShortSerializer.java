package com.lee.rpc.schema.primitives;

import com.lee.rpc.RpcException;
import com.lee.rpc.schema.Serializer;
import com.lee.rpc.util.stream.ByteBufInputStream;
import com.lee.rpc.util.stream.ByteBufOutputStream;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class ShortSerializer implements Serializer<Short> {

    public static final ShortSerializer SHORT_SERIALIZER = new ShortSerializer();

    private ShortSerializer() {
    }

    @Override
    public void serialize(Short obj, ByteBufOutputStream out) {
        try {
            out.writeShort(obj);
        } catch (IOException e) {
            throw new RpcException("Can not write short to output stream", e);
        }
    }

    @Override
    public Short deserialize(ByteBufInputStream in) {
        try {
            return in.readShort();
        } catch (IOException e) {
            throw new RpcException("Can not read short from input stream", e);
        }
    }
}
