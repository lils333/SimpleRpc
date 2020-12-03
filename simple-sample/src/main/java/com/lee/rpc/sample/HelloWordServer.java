package com.lee.rpc.sample;

import com.lee.rpc.RpcMethodUnit;
import com.lee.rpc.helper.RpcHelper;
import com.lee.rpc.helper.server.RpcServiceServerUnit;
import com.lee.rpc.helper.server.ServerHelper;

public class HelloWordServer implements HelloWorld, HelloWorld2 {

    @Override
    public Boolean sayHello(User user) {
        return user != null;
    }

    @Override
    public void sayHello(Integer integer) {

    }

    @Override
    public Integer sayHello() {
        return null;
    }

    @Override
    public void sayHello1() {

    }

    @Override
    public String saySay(Integer integer) {
        return "hello" + integer;
    }

    public static void main(String[] args) {

        HelloWordServer server = new HelloWordServer();

        ServerHelper.registerServer(server);
        ServerHelper.startRpcService();

        RpcServiceServerUnit serviceUnit = RpcHelper.getRpcServiceUnit(12);
        RpcMethodUnit inovker = serviceUnit.getMethodUnit((byte) 1);
        inovker.invoke(1);
    }
}
