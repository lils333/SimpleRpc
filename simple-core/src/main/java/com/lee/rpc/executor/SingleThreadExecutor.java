package com.lee.rpc.executor;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author l46li
 */
@Slf4j
public class SingleThreadExecutor extends AbstractExecutor {

    @Override
    protected Executor createExecutor() {
        return new ThreadPoolExecutor(
                1,
                1,
                60, TimeUnit.SECONDS,
                getBlockingQueue(),
                threadFactory,
                new ThreadPoolExecutor.AbortPolicy()
        );
    }
}
