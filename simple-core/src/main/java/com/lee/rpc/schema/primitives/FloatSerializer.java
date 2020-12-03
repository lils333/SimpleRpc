package com.lee.rpc.schema.primitives;

import com.lee.rpc.RpcException;
import com.lee.rpc.schema.Serializer;
import com.lee.rpc.util.stream.ByteBufInputStream;
import com.lee.rpc.util.stream.ByteBufOutputStream;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class FloatSerializer implements Serializer<Float> {

    public static final FloatSerializer FLOAT_SERIALIZER = new FloatSerializer();

    private FloatSerializer() {
    }

    @Override
    public void serialize(Float obj, ByteBufOutputStream out) {
        try {
            out.writeFloat(obj);
        } catch (IOException e) {
            throw new RpcException("Can not write float to output stream", e);
        }
    }

    @Override
    public Float deserialize(ByteBufInputStream in) {
        try {
            return in.readFloat();
        } catch (IOException e) {
            throw new RpcException("Can not read float from input stream", e);
        }
    }
}
