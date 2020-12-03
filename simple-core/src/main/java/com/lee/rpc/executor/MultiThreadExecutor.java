package com.lee.rpc.executor;

import com.lee.rpc.RpcException;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 通过控制队列的highWaterMark和lowWaterMark来间接的控制服务端流量，通过控制服务器端不在注册READ
 * 事件来间接的控制客服端的发送速度，服务器端不在注册READ事件就会导致底层的receiveBuffer满，而receiveBuffer满了以后
 * 就会导致客服端发送的sendBuffer满，那么就间接的控制了客服端发送的速度。
 *
 * @author Administrator
 */
@Slf4j
public class MultiThreadExecutor extends AbstractExecutor {

    private final int minThread;
    private final int maxThread;

    public MultiThreadExecutor(int minThread, int maxThread) {
        if (minThread < 0 || maxThread < 0) {
            throw new RpcException("minThread and maxThread must greater than 0");
        }

        if (maxThread < minThread) {
            throw new RpcException("maxThread must greater than equal minThread");
        }
        this.minThread = minThread;
        this.maxThread = maxThread;
    }

    @Override
    protected Executor createExecutor() {
        return new ThreadPoolExecutor(
                minThread,
                maxThread,
                60, TimeUnit.SECONDS,
                getBlockingQueue(),
                threadFactory,
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }
}
