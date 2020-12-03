package com.lee.rpc.annotation;

import com.lee.rpc.executor.OverflowMode;

import java.lang.annotation.*;

/**
 * @author l46li
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RpcMethod {

    /**
     * 指定是否共享线程模型,如果为true的话，那么同一个group的成员就共享同一个线程池去处理，如果为false，那么会为每一个方法
     * 创建一个线程池去处理
     * 如果不是线程池，那么为true表示该单线程是组内共享的，如果给false，那么会为每一个方法创建一个单独的线程去处理
     *
     * @return 是否为共享线程池模型
     */
    boolean sharedThreadPoolMode() default false;

    /**
     * 配合sharedThreadPoolMode一起使用，分组，同一个组里面的方法使用同一个线程池去调用
     *
     * @return 指定当前方法属于哪一个组
     */
    String group() default "default";

    /**
     * 指定线程池的最小线程数，-1表示单线程去处理
     *
     * @return 最小线程数量，
     */
    int minThread() default -1;

    /**
     * 指定线程池的最大线程数，-1表示单线程去处理
     *
     * @return 最大线程数
     */
    int maxThread() default -1;

    /**
     * 当前队列的大小
     *
     * @return 返回当前队列的大小
     */
    int capacity() default 10000;

    /**
     * 当前队列满了以后，应该采取的策略
     *
     * @return 返回具体的策略
     */
    OverflowMode overflowMode() default OverflowMode.DELAY;
}
