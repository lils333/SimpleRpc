package com.lee.rpc;

import com.lee.rpc.schema.RpcExceptionSchema;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * 后面实现一个RpcException包含ID的例子，把这些ID和错误原因一一对应，这样的话，服务端和客户端通过这个ID来统一消息
 * 这样的哈，服务器只需要发送ID过来，客户端就知道错误信息了，这样可以大量的减少服务器和客户端的消息大小
 *
 * @author l46li
 */
public class RpcExceptionSerializer {

    private static final ThreadLocal<LinkedBuffer> BUFFER = ThreadLocal.withInitial(() -> LinkedBuffer.allocate(4096));
    private static final RpcExceptionSchema schema = new RpcExceptionSchema();

    public void serialize(RpcException rpcException, OutputStream out) {
        LinkedBuffer buffer = BUFFER.get();
        try {
            ProtostuffIOUtil.writeTo(out, rpcException, schema, buffer);
        } catch (Exception e) {
            throw new RpcException(e);
        } finally {
            buffer.clear();
        }
    }

    public RpcException deserialize(InputStream in) {
        try {
            RpcException message = schema.newMessage();
            ProtostuffIOUtil.mergeFrom(in, message, schema);
            return message;
        } catch (Exception e) {
            throw new RpcException(e);
        }
    }
}
