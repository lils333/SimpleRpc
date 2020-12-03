package com.lee.rpc.helper;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 非常简单的一个ID递增生成器，主要是为了给method生成一个id，这个id最大不超过short的最大值
 *
 * @author Administrator
 */
@Slf4j
public class MethodIdGenerator {

    private final AtomicInteger idCounter = new AtomicInteger(0);

    public byte generateId() {
        int id = idCounter.incrementAndGet();
        if (id > Byte.MAX_VALUE) {
            throw new IllegalStateException("Can not support Byte.MAX_VALUE method id");
        }
        return (byte) id;
    }
}
