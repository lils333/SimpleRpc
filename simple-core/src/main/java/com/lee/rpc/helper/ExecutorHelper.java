package com.lee.rpc.helper;

import com.lee.rpc.executor.AbstractExecutor;
import com.lee.rpc.util.CityHash;
import io.netty.channel.ChannelId;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * 主要是为了方便其他RPC服务使用，也就是当前构建出来的Executor不仅仅只是给当前的RPC服务使用
 * 可以多个RPC服务共同使用一个线程池，这样就可以控制线程池的数量
 *
 * @author Administrator
 */
@Slf4j
public class ExecutorHelper {

    private static final Map<String, Executor> ALL_SHARED_EXECUTORS = new HashMap<>();
    private static final TreeMap<Long, Executor> KETAMA_EXECUTOR = new TreeMap<>();
    private static final Map<Thread, Executor> THREAD_MAPPING = new ConcurrentHashMap<>();
    private static final Map<ChannelId, Executor> CHANNEL_MAPPING = new ConcurrentHashMap<>();

    private ExecutorHelper() {
    }

    public static Executor obtainExecutor(String group) {
        return ALL_SHARED_EXECUTORS.get(group);
    }

    public static Executor obtainExecutorAccordingChannel(ChannelId channelId) {
        return CHANNEL_MAPPING.computeIfAbsent(channelId, ExecutorHelper::getKetamaExecutor);
    }

    public static void registerExecutor(String group, Executor executor) {
        ALL_SHARED_EXECUTORS.put(group, executor);
    }

    private static Executor getKetamaExecutor(ChannelId channelId) {
        byte[] bytes = channelId.asLongText().getBytes(UTF_8);
        Long key = CityHash.cityHash64(bytes, 0, bytes.length);
        if (KETAMA_EXECUTOR.containsKey(key)) {
            SortedMap<Long, Executor> tailMap = KETAMA_EXECUTOR.tailMap(key);
            if (tailMap.isEmpty()) {
                key = KETAMA_EXECUTOR.firstKey();
            } else {
                key = tailMap.firstKey();
            }
        }
        return KETAMA_EXECUTOR.get(key);
    }

    public static Set<ExecutorService> shutdown() {
        Set<ExecutorService> executors = new HashSet<>();
        //停止IO线程对应的业务线程
        for (Map.Entry<Thread, Executor> entry : THREAD_MAPPING.entrySet()) {
            stopExecutor(executors, entry.getValue());
        }
        //停止ChannelId对应的业务线程
        for (Map.Entry<Long, Executor> entry : KETAMA_EXECUTOR.entrySet()) {
            stopExecutor(executors, entry.getValue());
        }
        return executors;
    }

    private static void stopExecutor(Set<ExecutorService> executors, Executor executor) {
        if (executor instanceof AbstractExecutor) {
            AbstractExecutor abstractExecutor = (AbstractExecutor) executor;
            Executor internalExecutor = abstractExecutor.getInternalExecutor();
            if (internalExecutor instanceof ExecutorService) {
                executors.add((ExecutorService) internalExecutor);
            }
            abstractExecutor.stop();
        }
    }
}
