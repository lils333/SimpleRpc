package com.lee.rpc;

import com.lee.rpc.schema.RpcServiceSchema;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author Administrator
 */
@Slf4j
public class RpcMetadataSerializer {

    private static final ThreadLocal<LinkedBuffer> buffer = ThreadLocal.withInitial(() -> LinkedBuffer.allocate(4096));
    private static final RpcServiceSchema schema = new RpcServiceSchema();

    public void serialize(RpcService rpcService, OutputStream out) {
        LinkedBuffer linkedBuffer = buffer.get();
        try {
            ProtostuffIOUtil.writeTo(out, rpcService, schema, linkedBuffer);
        } catch (Exception e) {
            buffer.remove();
            throw new RpcException(e);
        } finally {
            linkedBuffer.clear();
        }
    }

    public RpcService deserialize(InputStream in) {
        try {
            RpcService rpcService = schema.newMessage();
            ProtostuffIOUtil.mergeFrom(in, rpcService, schema);
            return rpcService;
        } catch (Exception e) {
            throw new RpcException(e);
        }
    }
}
