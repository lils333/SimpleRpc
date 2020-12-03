package com.lee.rpc.schema.primitives;

import com.lee.rpc.RpcException;
import com.lee.rpc.schema.Serializer;
import com.lee.rpc.util.stream.ByteBufInputStream;
import com.lee.rpc.util.stream.ByteBufOutputStream;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class ByteSerializer implements Serializer<Byte> {

    public static final ByteSerializer BYTE_SERIALIZER = new ByteSerializer();

    private ByteSerializer() {
    }

    @Override
    public void serialize(Byte obj, ByteBufOutputStream out) {
        try {
            out.writeByte(obj);
        } catch (IOException e) {
            throw new RpcException("Can not write byte to output stream", e);
        }
    }

    @Override
    public Byte deserialize(ByteBufInputStream in) {
        try {
            return in.readByte();
        } catch (IOException e) {
            throw new RpcException("Can not read byte from input stream", e);
        }
    }
}
