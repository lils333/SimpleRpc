package com.lee.rpc.schema.primitives;

import com.lee.rpc.RpcException;
import com.lee.rpc.helper.recycler.ByteBufInputStreamRecycler;
import com.lee.rpc.helper.recycler.ByteBufOutputStreamRecycler;
import com.lee.rpc.schema.Serializer;
import com.lee.rpc.util.stream.ByteBufInputStream;
import com.lee.rpc.util.stream.ByteBufOutputStream;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;

import static com.lee.rpc.util.Constant.EMPTY_VALUE;
import static java.nio.charset.StandardCharsets.UTF_8;

@Slf4j
public class StringSerializer implements Serializer<String> {
    public static final StringSerializer STRING_SERIALIZER = new StringSerializer();

    @Override
    public void serialize(String obj, ByteBufOutputStream out) {
        try {
            if (obj == null || obj.isEmpty()) {
                out.writeShort(EMPTY_VALUE);
            } else {
                byte[] bytes = obj.getBytes(UTF_8);
                out.writeShort(bytes.length);
                out.write(bytes);
            }
        } catch (IOException e) {
            throw new RpcException("Can not write short to output stream", e);
        }
    }

    @Override
    public String deserialize(ByteBufInputStream in) {
        try {
            short length = in.readShort();
            if (length > 0) {
                byte[] bytes = new byte[length];
                int readNum = in.read(bytes);
                System.out.println("String length " + length);
                System.out.println("read bytes " + readNum);
                if (readNum != length) {
                    throw new IOException("Actual read bytes " + readNum + " < expected length " + length);
                }
                return new String(bytes, UTF_8);
            } else {
                return StringUtils.EMPTY;
            }
        } catch (IOException e) {
            throw new RpcException("Can not read short from input stream", e);
        }
    }

    public static void main(String[] args) {
        String s = "hello world";
        ByteBuf buffer = Unpooled.buffer();
        STRING_SERIALIZER.serialize(s, ByteBufOutputStreamRecycler.newInstance(buffer));
        System.out.println(s.length());
        System.out.println(STRING_SERIALIZER.deserialize(ByteBufInputStreamRecycler.newInstance(buffer)));
    }
}
