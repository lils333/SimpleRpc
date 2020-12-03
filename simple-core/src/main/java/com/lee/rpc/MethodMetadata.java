package com.lee.rpc;

/**
 * @author Administrator
 */
public class MethodMetadata {

    private byte methodId;
    private String returnType;
    private String methodName;
    private String parameterType;

    public byte getMethodId() {
        return methodId;
    }

    public void setMethodId(byte methodId) {
        this.methodId = methodId;
    }

    public String getReturnType() {
        return returnType;
    }

    public void setReturnType(String returnType) {
        this.returnType = returnType;
    }

    public String getMethodName() {
        return methodName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    public String getParameterType() {
        return parameterType;
    }

    public void setParameterType(String parameterType) {
        this.parameterType = parameterType;
    }
}
