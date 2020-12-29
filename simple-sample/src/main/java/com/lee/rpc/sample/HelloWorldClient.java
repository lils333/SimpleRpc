package com.lee.rpc.sample;

import com.lee.rpc.annotation.RpcClient;
import com.lee.rpc.annotation.RpcMethod;

@RpcClient(location = "simple://127.0.0.1:8080", service = "helloworld")
public interface HelloWorldClient {

    @RpcMethod
    Boolean sayHello(User user);

    @RpcMethod
    void sayHello(Integer integer);

    @RpcMethod
    Integer sayHello();

    @RpcMethod
    void sayHello1();
}