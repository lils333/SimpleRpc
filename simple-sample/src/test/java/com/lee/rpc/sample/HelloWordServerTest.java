package com.lee.rpc.sample;

import com.lee.rpc.helper.server.ServerHelper;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class HelloWordServerTest {

    @Test
    public void testServer() throws InterruptedException {
        HelloWordServer helloWordServer = new HelloWordServer();
        ServerHelper.registerServer(helloWordServer);
        ServerHelper.startRpcService();

        TimeUnit.MINUTES.sleep(30);
    }
}