package com.lee.rpc.schema;

import com.lee.rpc.MethodMetadata;
import com.lee.rpc.RpcMethodUnit;
import com.lee.rpc.RpcService;
import com.lee.rpc.helper.Weight;
import io.netty.util.collection.ByteObjectMap;
import io.protostuff.Input;
import io.protostuff.Output;
import io.protostuff.Schema;

import java.io.IOException;

/**
 * @author Administrator
 */
public class RpcServiceSchema implements Schema<RpcService> {

    private final MethodMetadataSchema schema = new MethodMetadataSchema();

    @Override
    public void mergeFrom(Input input, RpcService message) throws IOException {
        while (true) {
            int number = input.readFieldNumber(this);
            switch (number) {
                case 0:
                    return;
                case 1:
                    ByteObjectMap<RpcMethodUnit> allRpcMethodUnit = message.getAllRpcMethod();
                    MethodMetadata metadata
                            = input.mergeObject(schema.newMessage(), schema);
                    allRpcMethodUnit.put(
                            metadata.getMethodId(),
                            new RpcMethodUnit()
                                    .withMetadata(metadata)
                                    .withMethodId(metadata.getMethodId())
                                    .withReturnType(metadata.getReturnType())
                                    .withParameterType(metadata.getParameterType())
                    );
                    break;
                case 2:
                    message.setServiceId(input.readInt32());
                    break;
                case 3:
                    message.setWeight(Weight.toWeight(input.readInt32()));
                    break;
                default:
                    input.handleUnknownField(number, this);
            }
        }
    }

    @Override
    public void writeTo(Output output, RpcService message) throws IOException {
        ByteObjectMap<RpcMethodUnit> allRpcGroups = message.getAllRpcMethod();
        for (ByteObjectMap.PrimitiveEntry<RpcMethodUnit> next : allRpcGroups.entries()) {
            MethodMetadata metadata = next.value().getMetadata();
            output.writeObject(1, metadata, schema, true);
        }
        output.writeInt32(2, message.getServiceId(), false);
        output.writeInt32(3, message.getWeight().value(), false);
    }

    @Override
    public String getFieldName(int number) {
        switch (number) {
            case 1:
                return "allRpcGroups";
            case 2:
                return "serviceId";
            case 3:
                return "weight";
            default:
                return null;
        }
    }

    @Override
    public int getFieldNumber(String name) {
        switch (name) {
            case "allRpcGroups":
                return 1;
            case "serviceId":
                return 2;
            case "weight":
                return 3;
            default:
                return 0;
        }
    }

    @Override
    public boolean isInitialized(RpcService message) {
        return true;
    }

    @Override
    public RpcService newMessage() {
        return new RpcService();
    }

    @Override
    public String messageName() {
        return RpcService.class.getSimpleName();
    }

    @Override
    public String messageFullName() {
        return RpcService.class.getName();
    }

    @Override
    public Class<? super RpcService> typeClass() {
        return RpcService.class;
    }
}
