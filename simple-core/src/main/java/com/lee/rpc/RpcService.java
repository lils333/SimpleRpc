package com.lee.rpc;

import com.lee.rpc.annotation.RpcMethod;
import com.lee.rpc.executor.GroupThreadFactory;
import com.lee.rpc.executor.MultiThreadExecutor;
import com.lee.rpc.executor.OverflowMode;
import com.lee.rpc.executor.SingleThreadExecutor;
import com.lee.rpc.helper.ExecutorHelper;
import com.lee.rpc.helper.MethodIdGenerator;
import com.lee.rpc.helper.Weight;
import io.netty.util.collection.ByteObjectHashMap;
import io.netty.util.collection.ByteObjectMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

import static com.lee.rpc.helper.server.ServerHelper.SERVER_GENERATOR;

/**
 * 每一个使用@RpcServer注解标注的java对象都是一个RpcService对象，也就是负责提供Rpc服务的对象
 * 每一个RpcService对象里面有一系列的Method方法。每一个使用了@RpcMethod标注的方法都是一个
 * RpcMethodUnit，每一个RpcMethodUnit都属于一个组，并且有一个执行器，负责执行该Method
 *
 * @author l46li
 */
@Slf4j
public class RpcService {

    /**
     * 每一个ID的生成器都对应着一个RpcService，也就是每一个都是从1开始的数值
     */
    private final MethodIdGenerator idGenerator = new MethodIdGenerator();
    private final ByteObjectMap<RpcMethodUnit> allRpcMethod = new ByteObjectHashMap<>(8);
    private final ThreadFactory threadFactory = new GroupThreadFactory("nbi", false);

    private int serviceId;
    private int workers;
    private Weight weight;
    private String location;

    public void register(RpcMethod rpcMethod, Method method, Object instance, Class<?> inter) {
        String group = rpcMethod == null ? "default" : rpcMethod.group();
        byte methodId = idGenerator.generateId();

        //共享线程池模式
        if (rpcMethod != null && rpcMethod.sharedThreadPoolMode()) {
            Executor executor = ExecutorHelper.obtainExecutor(group);
            if (executor == null) {
                if (isSingleThread(rpcMethod)) {
                    executor = new SingleThreadExecutor()
                            .withOverflowMode(rpcMethod.overflowMode())
                            .withCapacity(rpcMethod.capacity())
                            .withThreadFactory(new GroupThreadFactory(group));

                } else {
                    executor = new MultiThreadExecutor(rpcMethod.minThread(), rpcMethod.maxThread())
                            .withCapacity(rpcMethod.capacity())
                            .withOverflowMode(rpcMethod.overflowMode())
                            .withThreadFactory(new GroupThreadFactory(group));
                }
                ExecutorHelper.registerExecutor(group, executor);
            }
            allRpcMethod.put(methodId,
                    SERVER_GENERATOR.generate(
                            inter, method, instance, executor, methodId, group, serviceId
                    )
            );
        } else {
            //接口上面的方法没有配置@RpcMethod, 默认也是一个Rpc提供的server
            if (rpcMethod == null) {
                allRpcMethod.put(methodId,
                        SERVER_GENERATOR.generate(
                                inter, method, instance,
                                new SingleThreadExecutor()
                                        .withCapacity(10000)
                                        .withOverflowMode(OverflowMode.DELAY)
                                        .withThreadFactory(threadFactory),
                                methodId, group, serviceId
                        )
                );
            } else {
                if (isSingleThread(rpcMethod)) {
                    allRpcMethod.put(methodId,
                            SERVER_GENERATOR.generate(
                                    inter, method, instance,
                                    new SingleThreadExecutor()
                                            .withCapacity(rpcMethod.capacity())
                                            .withOverflowMode(rpcMethod.overflowMode())
                                            .withThreadFactory(threadFactory),
                                    methodId, group, serviceId
                            )
                    );
                } else {
                    allRpcMethod.put(methodId,
                            SERVER_GENERATOR.generate(
                                    inter, method, instance,
                                    new MultiThreadExecutor(rpcMethod.minThread(), rpcMethod.maxThread())
                                            .withCapacity(rpcMethod.capacity())
                                            .withOverflowMode(rpcMethod.overflowMode())
                                            .withThreadFactory(threadFactory),
                                    methodId, group, serviceId
                            )
                    );
                }
            }
        }
    }

    public void mergeFrom(RpcService rpcService) {
        ByteObjectMap<RpcMethodUnit> rpcMethods = rpcService.getAllRpcMethod();
        for (ByteObjectMap.PrimitiveEntry<RpcMethodUnit> next : rpcMethods.entries()) {
            byte methodId = idGenerator.generateId();
            allRpcMethod.put(methodId, next.value().withMethodId(methodId));
        }
    }

    public Set<ExecutorService> shutdown() {
        Set<ExecutorService> executors = new HashSet<>();
        for (ByteObjectMap.PrimitiveEntry<RpcMethodUnit> next : allRpcMethod.entries()) {
            RpcMethodUnit rpcMethodUnit = next.value();
            ExecutorService internalExecutor = rpcMethodUnit.getInternalExecutor();
            if (internalExecutor != null) {
                executors.add(internalExecutor);
            }
            rpcMethodUnit.shutdown();
        }
        return executors;
    }

    public ByteObjectMap<RpcMethodUnit> getAllRpcMethod() {
        return allRpcMethod;
    }

    public int getServiceId() {
        return serviceId;
    }

    public void setServiceId(int serviceId) {
        this.serviceId = serviceId;
    }

    private boolean isSingleThread(RpcMethod rpcMethod) {
        return rpcMethod.maxThread() == -1 && rpcMethod.minThread() == -1;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RpcService that = (RpcService) o;

        return new EqualsBuilder()
                .append(serviceId, that.serviceId)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(serviceId)
                .toHashCode();
    }

    public void setWorkers(int workers) {
        this.workers = workers;
    }

    public int getWorkers() {
        return this.workers;
    }

    public void setWeight(Weight weight) {
        this.weight = weight;
    }

    public Weight getWeight() {
        return weight;
    }

    @Override
    public String toString() {
        return "RpcService{" +
                "idGenerator=" + idGenerator +
                ", allRpcMethod=" + allRpcMethod +
                ", serviceId=" + serviceId +
                ", workers=" + workers +
                ", location='" + location + '\'' +
                '}';
    }
}
