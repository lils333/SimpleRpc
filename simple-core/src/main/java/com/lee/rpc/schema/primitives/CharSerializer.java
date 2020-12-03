package com.lee.rpc.schema.primitives;

import com.lee.rpc.RpcException;
import com.lee.rpc.schema.Serializer;
import com.lee.rpc.util.stream.ByteBufInputStream;
import com.lee.rpc.util.stream.ByteBufOutputStream;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class CharSerializer implements Serializer<Character> {

    public static final CharSerializer CHAR_SERIALIZER = new CharSerializer();

    private CharSerializer() {
    }

    @Override
    public void serialize(Character obj, ByteBufOutputStream out) {
        try {
            out.writeChar(obj);
        } catch (IOException e) {
            throw new RpcException("Can not write char to output stream", e);
        }
    }

    @Override
    public Character deserialize(ByteBufInputStream in) {
        try {
            return in.readChar();
        } catch (IOException e) {
            throw new RpcException("Can not read char from input stream", e);
        }
    }
}
