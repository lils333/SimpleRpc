package com.lee.rpc.sample;

import com.lee.rpc.annotation.RpcMethod;
import com.lee.rpc.annotation.RpcServer;

@RpcServer(publish = "192.168.1.105:8080", service = "helloworld2")
public interface HelloWorld2 {

    @RpcMethod
    String saySay(Integer integer);

}
