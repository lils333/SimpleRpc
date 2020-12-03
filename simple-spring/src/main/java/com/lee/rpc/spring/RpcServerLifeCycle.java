package com.lee.rpc.spring;//package com.lee.netty.spring;
//
//import com.google.common.net.InetAddresses;
//import com.lee.netty.RpcService;
//import com.lee.netty.annotation.RpcMethod;
//import com.lee.netty.annotation.RpcServer;
//import com.lee.netty.exception.RpcException;
//import com.lee.netty.helper.RpcHelper;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.commons.lang3.builder.EqualsBuilder;
//import org.apache.commons.lang3.builder.HashCodeBuilder;
//import org.springframework.beans.BeansException;
//import org.springframework.beans.factory.BeanCreationNotAllowedException;
//import org.springframework.beans.factory.config.BeanPostProcessor;
//import org.springframework.context.SmartLifecycle;
//import org.springframework.stereotype.Component;
//
//import java.lang.reflect.Method;
//import java.net.InetAddress;
//import java.net.InetSocketAddress;
//import java.util.HashSet;
//import java.util.Set;
//
///**
// * @author l46li
// */
//@Component
//@Slf4j
//public class RpcServerLifeCycle implements SmartLifecycle, BeanPostProcessor {
//
//    private final Set<RpcServerWrapper> rpcServers = new HashSet<>();
//
//    private boolean isRunning;
//
//    @Override
//    public void start() {
//        isRunning = true;
//
//        //启动的时候在register，主要是为了保证在多个不同的系统上面运行的时候，保证顺序一致性，只要保证了顺序一致性
//        //那么方法的ID也就是保证了顺序
//        registerRpcService();
//
//        //注册完成以后，在来启动
//        RpcHelper.startRpcService();
//
//        //完事以后就清空
//        rpcServers.clear();
//
//        log.info("RpcService is stared");
//    }
//
//    /**
//     * phase 启动的额时候是按照 从小--->到大， 关闭的时候是按照从大--->到小 来关闭，如果有depency，那么depency优先
//     * 注意每一个phase，对应着一个LifecycleGroup，也就是每一个phase相同的Lifecycle的Bean他们都属于同一个LifecycleGroup
//     * 在Spring stop的时候开，是按照一个phase一个phase来关闭的，也就是按照LifecycleGroup来stop同一个组里面的所有Bean
//     * 每一个phase关闭完成以后，都需要等待，timeoutPerShutdownPhases指定的时间，因为SmartLifecycle的stop是可以使用
//     * 线程池去关闭的，也即是可以异步去关闭，所以需要指定一个关闭的最大时间。
//     * 其实内部使用的是CountDownLatch来实现的，也就是同一个LifecycleGroup里面的所有成员，每关闭一个就countLatch一下，
//     * 因为存储异步关闭的可能，所以这个地方就使用了CountDownLatch的await去等待timeoutPerShutdownPhases去关闭，如果
//     * CountDownLatch计数为0，也就是LifecycleGroup里面的所有对象都关闭完成了，那么就不在阻塞，如果timeoutPerShutdownPhases
//     * 时间过了，都还没有关闭完成，那么就打印一个提示消息，然后继续关闭其他的LifecycleGroup
//     */
//    @Override
//    public void stop() {
//        isRunning = false;
//
//        RpcHelper.stopRpcService();
//
//        log.info("RpcService is stopped");
//    }
//
//    @Override
//    public boolean isRunning() {
//        return isRunning;
//    }
//
//    @Override
//    public int getPhase() {
//        return 3;
//    }
//
//    @Override
//    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
//        RpcServer rpcServer = bean.getClass().getAnnotation(RpcServer.class);
//        if (rpcServer != null) {
//            RpcServerWrapper rpcServerWrapper = new RpcServerWrapper();
//            rpcServerWrapper.rpcServer = rpcServer;
//            rpcServerWrapper.instance = bean;
//            rpcServerWrapper.serviceId = rpcServer.serviceId();
//            rpcServerWrapper.id = rpcServer.id();
//
//            //如果已经存在，那么就抛出异常
//            if (!rpcServers.add(rpcServerWrapper)) {
//                throw new BeanCreationNotAllowedException(beanName,
//                        "RpcServer with id " + rpcServerWrapper.id + " has multiple Server " + bean);
//            }
//        }
//        return bean;
//    }
//
//    private void registerRpcService() {
//        rpcServers.stream().sorted().forEach(
//                rpcServerWrapper -> {
//                    RpcService service = new RpcService();
//                    service.setLocation(rpcServerWrapper.rpcServer.location());
//                    service.setServiceId(rpcServerWrapper.serviceId);
//                    Method[] declaredMethods = rpcServerWrapper.instance.getClass().getDeclaredMethods();
//                    for (Method declaredMethod : declaredMethods) {
//                        RpcMethod rpcMethod = declaredMethod.getDeclaredAnnotation(RpcMethod.class);
//                        if (rpcMethod != null) {
//                            service.register(rpcMethod, declaredMethod, rpcServerWrapper.instance, rpcServerWrapper);
//                        }
//                    }
//                    RpcHelper.registerServer(
//                            createInetAddress(rpcServerWrapper.rpcServer.publish()), service
//                    );
//                }
//        );
//    }
//
//    private static InetSocketAddress createInetAddress(String publish) {
//        int index = publish.lastIndexOf(":");
//        if (index != -1) {
//            return new InetSocketAddress(
//                    getHostName(publish.substring(0, index)), getPort(publish.substring(index + 1))
//            );
//        } else {
//            throw new RpcException("Invalid publish address, valid address format hostname:port");
//        }
//    }
//
//    private static InetAddress getHostName(String hostname) {
//        try {
//            return InetAddresses.forString(hostname);
//        } catch (Exception e) {
//            throw new RpcException("hostname is invalid", e);
//        }
//    }
//
//    private static short getPort(String port) {
//        try {
//            return Short.parseShort(port);
//        } catch (NumberFormatException e) {
//            throw new RpcException("Invalid port, valid port valid is 1024 - 65535", e);
//        }
//    }
//
//    static class RpcServerWrapper implements Comparable<RpcServerWrapper> {
//        RpcServer rpcServer;
//        Object instance;
//        int serviceId;
//        int id;
//
//
//        @Override
//        public boolean equals(Object o) {
//            if (this == o) {
//                return true;
//            }
//
//            if (o == null || getClass() != o.getClass()) {
//                return true;
//            }
//
//            RpcServerWrapper that = (RpcServerWrapper) o;
//
//            return new EqualsBuilder()
//                    .append(id, that.id)
//                    .isEquals();
//        }
//
//        @Override
//        public int hashCode() {
//            return new HashCodeBuilder(17, 37)
//                    .append(id)
//                    .toHashCode();
//        }
//
//        @Override
//        public int compareTo(RpcServerWrapper wrapper) {
//            if (wrapper == null || this == wrapper) {
//                return 1;
//            }
//            return this.id - wrapper.id;
//        }
//    }
//}
