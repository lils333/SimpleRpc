package com.lee.rpc.schema.primitives;

import com.lee.rpc.RpcException;
import com.lee.rpc.schema.Serializer;
import com.lee.rpc.util.stream.ByteBufInputStream;
import com.lee.rpc.util.stream.ByteBufOutputStream;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class BooleanSerializer implements Serializer<Boolean> {

    public static final BooleanSerializer BOOLEAN_SERIALIZER = new BooleanSerializer();

    private BooleanSerializer() {
    }

    @Override
    public void serialize(Boolean obj, ByteBufOutputStream out) {
        try {
            out.writeBoolean(obj);
        } catch (IOException e) {
            throw new RpcException("Can not write boolean to output stream", e);
        }
    }

    @Override
    public Boolean deserialize(ByteBufInputStream in) {
        try {
            return in.readBoolean();
        } catch (IOException e) {
            throw new RpcException("Can not read boolean from input stream", e);
        }
    }
}
