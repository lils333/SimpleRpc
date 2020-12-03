package com.lee.rpc.schema.primitives;

import com.lee.rpc.RpcException;
import com.lee.rpc.schema.Serializer;
import com.lee.rpc.util.stream.ByteBufInputStream;
import com.lee.rpc.util.stream.ByteBufOutputStream;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class IntSerializer implements Serializer<Integer> {

    public static final IntSerializer INT_SERIALIZER = new IntSerializer();

    private IntSerializer() {
    }

    @Override
    public void serialize(Integer obj, ByteBufOutputStream out) {
        try {
            out.writeInt(obj);
        } catch (IOException e) {
            throw new RpcException("Can not write int to output stream", e);
        }
    }

    @Override
    public Integer deserialize(ByteBufInputStream in) {
        try {
            return in.readInt();
        } catch (IOException e) {
            throw new RpcException("Can not read short from input stream", e);
        }
    }
}
