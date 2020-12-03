package com.lee.rpc.schema;

import com.lee.rpc.RpcException;
import com.lee.rpc.util.exception.ErrorType;
import io.protostuff.Input;
import io.protostuff.Output;
import io.protostuff.Schema;

import java.io.IOException;

/**
 * @author Administrator
 */
public class RpcExceptionSchema implements Schema<RpcException> {

    @Override
    public void mergeFrom(Input input, RpcException message) throws IOException {
        while (true) {
            int number = input.readFieldNumber(this);
            switch (number) {
                case 0:
                    return;
                case 1:
                    int index = input.readInt32();
                    for (ErrorType value : ErrorType.values()) {
                        if (value.ordinal() == index) {
                            message.withStatus(value);
                            break;
                        }
                    }
                    break;
                case 2:
                    message.withError(input.readString());
                    break;
                default:
                    input.handleUnknownField(number, this);
            }
        }
    }

    @Override
    public void writeTo(Output output, RpcException message) throws IOException {
        output.writeInt32(1, message.getStatus().ordinal(), false);
        output.writeString(2, message.getMessage(), false);
    }

    @Override
    public String getFieldName(int number) {
        switch (number) {
            case 1:
                return "status";
            case 2:
                return "message";
            default:
                return null;
        }
    }

    @Override
    public int getFieldNumber(String name) {
        switch (name) {
            case "status":
                return 1;
            case "message":
                return 2;
            default:
                return 0;
        }
    }

    @Override
    public boolean isInitialized(RpcException message) {
        return true;
    }

    @Override
    public RpcException newMessage() {
        return new RpcException();
    }

    @Override
    public String messageName() {
        return RpcException.class.getSimpleName();
    }

    @Override
    public String messageFullName() {
        return RpcException.class.getName();
    }

    @Override
    public Class<? super RpcException> typeClass() {
        return RpcException.class;
    }
}
