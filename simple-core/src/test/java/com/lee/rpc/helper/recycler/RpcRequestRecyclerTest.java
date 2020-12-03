package com.lee.rpc.helper.recycler;

import com.lee.rpc.RpcRequest;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

public class RpcRequestRecyclerTest {


    @Test
    public void testRecyler() throws InterruptedException {

        final RpcRequest rpcRequestt = RpcRequestRecycler.newInstance(1, 2L, (byte) 1);

        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                RpcRequestRecycler.recycle(rpcRequestt);
                RpcRequest rpcRequest = RpcRequestRecycler.newInstance(1, 2L, (byte) 1);
                assertNotSame(rpcRequestt, rpcRequest);
                RpcRequestRecycler.recycle(rpcRequest);
            }
        });
        thread.start();

        TimeUnit.SECONDS.sleep(1);

        RpcRequest rpcRequest1 = RpcRequestRecycler.newInstance(1, 2L, (byte) 1);
        RpcRequest rpcRequest2 = RpcRequestRecycler.newInstance(1, 2L, (byte) 1);
        assertSame(rpcRequestt, rpcRequest1);
        assertNotSame(rpcRequestt, rpcRequest2);
        RpcRequestRecycler.recycle(rpcRequest1);
        RpcRequestRecycler.recycle(rpcRequest2);
    }
}