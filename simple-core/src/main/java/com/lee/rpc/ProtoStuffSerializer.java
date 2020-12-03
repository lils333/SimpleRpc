package com.lee.rpc;

import com.lee.rpc.schema.Serializer;
import com.lee.rpc.util.stream.ByteBufInputStream;
import com.lee.rpc.util.stream.ByteBufOutputStream;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import lombok.extern.slf4j.Slf4j;

/**
 * @author l46li
 */
@Slf4j
public class ProtoStuffSerializer<S> implements Serializer<S> {

    private static final ThreadLocal<LinkedBuffer> bufferCache = ThreadLocal.withInitial(() -> LinkedBuffer.allocate(4096));
    private final Schema<S> schema;

    public ProtoStuffSerializer(Class<S> type) {
        schema = RuntimeSchema.getSchema(type);
    }

    @Override
    public void serialize(S object, ByteBufOutputStream out) {
        LinkedBuffer buffer = bufferCache.get();
        try {
            ProtostuffIOUtil.writeTo(out, object, schema, buffer);
        } catch (Exception e) {
            bufferCache.remove();
            throw new RpcException("Can not serialize object : " + object, e);
        } finally {
            buffer.clear();
        }
    }

    @Override
    public S deserialize(ByteBufInputStream in) {
        try {
            S value = schema.newMessage();
            ProtostuffIOUtil.mergeFrom(in, value, schema);
            return value;
        } catch (Exception e) {
            throw new RpcException("Can not deserialize from bytes with schema " + schema, e);
        }
    }
}
