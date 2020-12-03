package com.lee.rpc.schema;


import com.lee.rpc.util.stream.ByteBufInputStream;
import com.lee.rpc.util.stream.ByteBufOutputStream;

public interface Serializer<S> {

    void serialize(S obj, ByteBufOutputStream out);

    S deserialize(ByteBufInputStream in);
}
