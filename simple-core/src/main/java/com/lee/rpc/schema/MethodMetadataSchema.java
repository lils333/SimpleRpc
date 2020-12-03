package com.lee.rpc.schema;

import com.lee.rpc.MethodMetadata;
import io.protostuff.Input;
import io.protostuff.Output;
import io.protostuff.Schema;

import java.io.IOException;
import java.nio.ByteBuffer;

import static com.lee.rpc.util.Constant.EMPTY;

/**
 * @author Administrator
 */
public class MethodMetadataSchema implements Schema<MethodMetadata> {

    @Override
    public void mergeFrom(Input input, MethodMetadata message) throws IOException {
        while (true) {
            int number = input.readFieldNumber(this);
            switch (number) {
                case 0:
                    return;
                case 1:
                    message.setMethodName(input.readString());
                    break;
                case 2:
                    message.setReturnType(input.readString());
                    break;
                case 3:
                    message.setParameterType(input.readString());
                    break;
                case 4:
                    ByteBuffer byteBuffer = input.readByteBuffer();
                    message.setMethodId(byteBuffer.get());
                    break;
                default:
                    input.handleUnknownField(number, this);
            }
        }
    }

    @Override
    public void writeTo(Output output, MethodMetadata message) throws IOException {
        output.writeString(1, message.getMethodName(), false);
        output.writeString(2,
                message.getReturnType() != null ? message.getReturnType() : EMPTY, false);
        output.writeString(3,
                message.getParameterType() != null ? message.getParameterType() : EMPTY, false);
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put(message.getMethodId());
        buffer.flip();
        output.writeBytes(4, buffer, false);
    }

    @Override
    public String getFieldName(int number) {
        switch (number) {
            case 1:
                return "methodName";
            case 2:
                return "returnType";
            case 3:
                return "parameterType";
            case 4:
                return "methodId";
            default:
                return null;
        }
    }

    @Override
    public int getFieldNumber(String name) {
        switch (name) {
            case "methodName":
                return 1;
            case "returnType":
                return 2;
            case "parameterType":
                return 3;
            case "methodId":
                return 4;
            default:
                return 0;
        }
    }

    @Override
    public boolean isInitialized(MethodMetadata message) {
        return true;
    }

    @Override
    public MethodMetadata newMessage() {
        return new MethodMetadata();
    }

    @Override
    public String messageName() {
        return MethodMetadata.class.getSimpleName();
    }

    @Override
    public String messageFullName() {
        return MethodMetadata.class.getName();
    }

    @Override
    public Class<? super MethodMetadata> typeClass() {
        return MethodMetadata.class;
    }
}
