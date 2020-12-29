package com.lee.rpc.sample;

import com.lee.rpc.annotation.RpcMethod;
import com.lee.rpc.annotation.RpcServer;

@RpcServer(publish = "127.0.0.1:8080", service = "helloworld2")
public interface HelloWorld2 {

    @RpcMethod
    String saySay(Integer integer);

}
