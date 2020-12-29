package com.lee.rpc.sample;

import com.lee.rpc.annotation.RpcClient;
import com.lee.rpc.annotation.RpcMethod;

@RpcClient(location = "simple://127.0.0.1:8080", service = "helloworld2")
public interface HelloWorldClient2 {

    @RpcMethod
    String saySay(Integer integer);
}
