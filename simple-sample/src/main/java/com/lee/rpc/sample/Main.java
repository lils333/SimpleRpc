package com.lee.rpc.sample;

import com.lee.rpc.helper.server.ServerHelper;

import java.util.concurrent.TimeUnit;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        HelloWordServer helloWordServer = new HelloWordServer();
        ServerHelper.registerServer(helloWordServer);
        ServerHelper.startRpcService();
        TimeUnit.MINUTES.sleep(30);
    }
}
