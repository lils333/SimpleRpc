package com.lee.rpc.schema.primitives;

import com.lee.rpc.RpcException;
import com.lee.rpc.schema.Serializer;
import com.lee.rpc.util.stream.ByteBufInputStream;
import com.lee.rpc.util.stream.ByteBufOutputStream;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class DoubleSerializer implements Serializer<Double> {

    public static final DoubleSerializer DOUBLE_SERIALIZER = new DoubleSerializer();

    private DoubleSerializer() {
    }

    @Override
    public void serialize(Double obj, ByteBufOutputStream out) {
        try {
            out.writeDouble(obj);
        } catch (IOException e) {
            throw new RpcException("Can not write double to output stream", e);
        }
    }

    @Override
    public Double deserialize(ByteBufInputStream in) {
        try {
            return in.readDouble();
        } catch (IOException e) {
            throw new RpcException("Can not read double from input stream", e);
        }
    }
}
