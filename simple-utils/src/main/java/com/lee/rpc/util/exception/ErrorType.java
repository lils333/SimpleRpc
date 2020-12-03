package com.lee.rpc.util.exception;

/**
 * @author l46li
 */

public enum ErrorType {
    /**
     * 定义错误的类型
     */
    CLIENT_ERROR,
    RPC_SERVER_STOP,
    RPC_CLIENT_STOP,
    SERVER_ERROR,
    SERIALIZER_ERROR,
    NOT_SUPPORT_TYPE,
    SERVICE_BUSY,
    CLIENT_SERIALIZER_ERROR,
    NOT_EXIST_SERVICE_ID
}
