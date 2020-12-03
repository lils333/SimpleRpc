package com.lee.rpc.spring;

import com.google.common.hash.Hashing;
import com.google.common.net.InetAddresses;
import com.lee.rpc.RpcException;
import com.lee.rpc.RpcService;
import com.lee.rpc.annotation.RpcMethod;
import com.lee.rpc.annotation.RpcServer;
import com.lee.rpc.helper.RpcHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author l46li
 */
@Component
@Slf4j
public class RpcServerLifeCycle implements SmartLifecycle, BeanPostProcessor {

    private static final List<RpcServerWrapper> RPC_SERVER_WRAPPERS = new ArrayList<>();

    private boolean isRunning;

    @Override
    public void start() {
        isRunning = true;

        // 启动的时候在register，主要是为了保证在多个不同的系统上面运行的时候，保证顺序一致性，只要保证了顺序一致性
        // 那么方法的ID也就是保证了顺序
        registerRpcService();

        //注册完成以后，在来启动
        RpcHelper.startRpcService();

        //完事以后就清空
        RPC_SERVER_WRAPPERS.clear();

        log.info("RpcService is stared");
    }

    /**
     * phase 启动的额时候是按照 从小--->到大， 关闭的时候是按照从大--->到小 来关闭，如果有depency，那么depency优先
     * 注意每一个phase，对应着一个LifecycleGroup，也就是每一个phase相同的Lifecycle的Bean他们都属于同一个LifecycleGroup
     * 在Spring stop的时候开，是按照一个phase一个phase来关闭的，也就是按照LifecycleGroup来stop同一个组里面的所有Bean
     * 每一个phase关闭完成以后，都需要等待，timeoutPerShutdownPhases指定的时间，因为SmartLifecycle的stop是可以使用
     * 线程池去关闭的，也即是可以异步去关闭，所以需要指定一个关闭的最大时间。
     * 其实内部使用的是CountDownLatch来实现的，也就是同一个LifecycleGroup里面的所有成员，每关闭一个就countLatch一下，
     * 因为存储异步关闭的可能，所以这个地方就使用了CountDownLatch的await去等待timeoutPerShutdownPhases去关闭，如果
     * CountDownLatch计数为0，也就是LifecycleGroup里面的所有对象都关闭完成了，那么就不在阻塞，如果timeoutPerShutdownPhases
     * 时间过了，都还没有关闭完成，那么就打印一个提示消息，然后继续关闭其他的LifecycleGroup
     */
    @Override
    public void stop() {
        isRunning = false;

        RpcHelper.stopRpcService();

        log.info("RpcService is stopped");
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        HashSet<Class<?>> results = new HashSet<>();
        findRpcServer(bean.getClass(), results);
        if (results.isEmpty()) {
            throw new RpcException("@RpcServer must annotated on an interface");
        } else {
            for (Class<?> targetClass : results) {
                RpcServer rpcServer = targetClass.getDeclaredAnnotation(RpcServer.class);
                RpcServerWrapper rpcServerWrapper = new RpcServerWrapper();
                rpcServerWrapper.rpcServer = rpcServer;
                rpcServerWrapper.instance = bean;
                rpcServerWrapper.inter = targetClass;
                rpcServerWrapper.serviceId = generateServiceId(rpcServer.service());
                rpcServerWrapper.workers = rpcServer.works();
                RPC_SERVER_WRAPPERS.add(rpcServerWrapper);
            }
        }
        log.info("Find @RpcServer bean with name {} on Spring {}", beanName, bean);
        return bean;
    }

    /**
     * 获取所有@RpcServer的接口
     *
     * @param targetInterface 需要查找的类
     */
    private static void findRpcServer(Class<?> targetInterface, Set<Class<?>> results) {
        Class<?>[] interfaces = targetInterface.getInterfaces();
        for (Class<?> targetClass : interfaces) {
            RpcServer rpcServer = targetClass.getDeclaredAnnotation(RpcServer.class);
            if (rpcServer != null) {
                results.add(targetClass);
            } else {
                findRpcServer(targetClass, results);
            }
        }
    }

    private static int generateServiceId(String serviceName) {
        int serviceId = Hashing.murmur3_32().hashString(serviceName, UTF_8).asInt();
        while (serviceId < 0) {
            serviceId = Hashing.murmur3_32().hashString(serviceName + serviceId, UTF_8).asInt();
        }
        return serviceId;
    }

    private void registerRpcService() {
        RPC_SERVER_WRAPPERS.stream().sorted().forEach(
                rpcServerWrapper -> {
                    RpcService service = new RpcService();
                    service.setLocation(rpcServerWrapper.rpcServer.location());
                    service.setServiceId(rpcServerWrapper.serviceId);
                    service.setWorkers(rpcServerWrapper.workers);
                    service.setWeight(rpcServerWrapper.rpcServer.weight());
                    Method[] declaredMethods = rpcServerWrapper.inter.getDeclaredMethods();

                    //使用方法签名来排序，这样保证在其他地方这个顺序也是一致的
                    Arrays.sort(declaredMethods, Comparator.comparingInt(o -> generateServiceId(o.toString())));

                    for (Method declaredMethod : declaredMethods) {
                        RpcMethod rpcMethod = declaredMethod.getDeclaredAnnotation(RpcMethod.class);
                        if (rpcMethod != null) {
                            service.register(
                                    rpcMethod, declaredMethod, rpcServerWrapper.instance, rpcServerWrapper.inter
                            );
                        }
                    }
                    RpcHelper.registerServer(
                            createInetAddress(rpcServerWrapper.rpcServer.publish()), service
                    );
                }
        );
    }

    private static InetSocketAddress createInetAddress(String publish) {
        int index = publish.lastIndexOf(":");
        if (index != -1) {
            return new InetSocketAddress(
                    getHostName(publish.substring(0, index)), getPort(publish.substring(index + 1))
            );
        } else {
            throw new RpcException("Invalid publish address, valid address format hostname:port");
        }
    }

    private static InetAddress getHostName(String hostname) {
        try {
            return InetAddresses.forString(hostname);
        } catch (Exception e) {
            throw new RpcException("hostname is invalid", e);
        }
    }

    private static short getPort(String port) {
        try {
            return Short.parseShort(port);
        } catch (NumberFormatException e) {
            throw new RpcException("Invalid port, valid port valid is 1024 - 65535", e);
        }
    }

    private static class RpcServerWrapper implements Comparable<RpcServerWrapper> {
        int workers;
        Class<?> inter;
        RpcServer rpcServer;
        Object instance;
        int serviceId;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return true;
            }

            RpcServerWrapper that = (RpcServerWrapper) o;

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

        @Override
        public int compareTo(RpcServerWrapper wrapper) {
            if (wrapper == null || this == wrapper) {
                return 1;
            }
            return this.serviceId - wrapper.serviceId;
        }

        @Override
        public String toString() {
            return "RpcServerWrapper{" +
                    "id=" + serviceId +
                    '}';
        }
    }
}
