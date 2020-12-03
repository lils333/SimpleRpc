package com.lee.rpc.sample;

import com.lee.rpc.annotation.RpcMethod;
import com.lee.rpc.annotation.RpcServer;
import com.lee.rpc.helper.Weight;

@RpcServer(publish = "127.0.0.1:8080", service = "helloworld", weight = Weight.ONE)
public interface HelloWorld {

    @RpcMethod(minThread = 4, maxThread = 4)
    Boolean sayHello(User user);

    @RpcMethod
    void sayHello(Integer integer);

    @RpcMethod
    Integer sayHello();

    @RpcMethod
    void sayHello1();
}
