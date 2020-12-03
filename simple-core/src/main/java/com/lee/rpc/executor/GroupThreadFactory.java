package com.lee.rpc.executor;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author l46li
 */
public class GroupThreadFactory implements ThreadFactory {

    private static final AtomicInteger POOL_NUMBER = new AtomicInteger(1);
    private final ThreadGroup group;
    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final String namePrefix;

    public GroupThreadFactory(String groupName) {
        this(groupName, true);
    }

    public GroupThreadFactory(String groupName, boolean shared) {
        SecurityManager s = System.getSecurityManager();
        group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
        if (shared) {
            namePrefix = groupName + "-shared-";
        } else {
            namePrefix = groupName + "_";
        }
    }

    @Override
    public Thread newThread(Runnable task) {
        Thread t = new Thread(group, task, namePrefix + threadNumber.getAndIncrement(), 0);
        if (t.isDaemon()) {
            t.setDaemon(false);
        }
        if (t.getPriority() != Thread.NORM_PRIORITY) {
            t.setPriority(Thread.NORM_PRIORITY);
        }
        return t;
    }
}
