package com.lee.rpc.sample;

import com.lee.rpc.annotation.RpcClient;
import com.lee.rpc.annotation.RpcMethod;

@RpcClient(location = "simple://192.168.1.105:8080", service = "helloworld2")
public interface HelloWorldClient2 {

    @RpcMethod
    String saySay(Integer integer);
}
