package com.lee.rpc;

import com.lee.rpc.util.exception.ErrorType;

import static com.lee.rpc.util.exception.ErrorType.CLIENT_ERROR;


/**
 * @author Administrator
 */
public class RpcException extends RuntimeException {

    private ErrorType status = CLIENT_ERROR;
    private String message;
    private RpcRequest request;

    public RpcException() {
        super();
    }

    public RpcException(String message) {
        super(message);
        this.message = message;
    }

    public RpcException(String message, Throwable cause) {
        super(message, cause);
        this.message = message;
    }

    public RpcException(Throwable cause) {
        super(cause);
        this.message = cause.getMessage();
    }

    protected RpcException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
        this.message = message;
    }

    public RpcException withStatus(ErrorType errorType) {
        this.status = errorType;
        return this;
    }

    public RpcException withError(String message) {
        this.message = message;
        return this;
    }

    public ErrorType getStatus() {
        return status;
    }

    public void setStatus(ErrorType status) {
        this.status = status;
    }

    @Override
    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public RpcRequest getRpcRequest() {
        return this.request;
    }

    public RpcException withRequest(RpcRequest request) {
        this.request = request;
        return this;
    }
}
