package com.lee.rpc.helper.client;

import com.google.common.hash.Hashing;
import com.google.common.net.InetAddresses;
import com.lee.rpc.RpcException;
import com.lee.rpc.annotation.RpcClient;
import com.lee.rpc.helper.RpcHelper;
import lombok.extern.slf4j.Slf4j;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.FutureTask;

import static com.lee.rpc.helper.RpcHelper.ZOOKEEPER_PREFIX;
import static com.lee.rpc.util.exception.ErrorType.RPC_CLIENT_STOP;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * 帮组我们创建代理类
 *
 * @author Administrator
 */
@Slf4j
public class ClientHelper {

    public static final RpcClientGenerator CLIENT_GENERATOR = new RpcClientGenerator();

    private static final String LOCAL = "simple://";

    /**
     * 只负责创建连接，不负责调用，所以全局只有一个就可以了
     */
    private static final NettyClient NETTY_CLIENT = new NettyClient();

    /**
     * 使用ConcurrentHashMap的目的是减少锁的竞争，也就是同一个hash桶里面才存在锁的竞争
     */
    private static final Map<Long, Task> RESULTS = new ConcurrentHashMap<>();

    /**
     * 记录一下当前客户端的实际个数，也就是每一个serviceId都对应着一个ClientProxy
     */
    private static final Map<Integer, ClientProxy> CLIENTS = new ConcurrentHashMap<>();

    /**
     * 一个假的task，什么事情都不做
     */
    private static final Callable<Object> DUMMY_TASK = () -> null;

    private ClientHelper() {
    }

    public static class Task extends FutureTask<Object> {

        public Task() {
            super(DUMMY_TASK);
        }

        public void setValue(Object object) {
            if (object instanceof RpcException) {
                super.setException((RpcException) object);
            } else {
                super.set(object);
            }
        }
    }

    public static void removeTask(Long requestId) {
        RESULTS.remove(requestId);
    }

    public static Task putTask(Long requestId) {
        Task task = new Task();
        RESULTS.putIfAbsent(requestId, task);
        return task;
    }

    public static void setValue(Long requestId, Object value) {
        Task task = RESULTS.remove(requestId);
        if (task != null) {
            task.setValue(value);
        } else {
            log.warn(
                    "RequestId {} does not exist, but service has processed, maybe current operation executing timeout, " +
                            "retry another request send to service",
                    requestId
            );
        }
    }

    public static void shutdown() {
        for (Map.Entry<Integer, ClientProxy> entry : CLIENTS.entrySet()) {
            log.info("Prepared to stop service {}", entry.getKey());
            ClientProxy clientProxy = entry.getValue();
            clientProxy.setShutdown(true);
        }

        NETTY_CLIENT.shutdown();

        //把还没有处理的task直接设置成失败，可能存在服务器已经接受到了请求，正在处理，或者服务器端已经处理完成了,准备回复
        //这个地方只是客户端的收尾工作，也就是把那些没有及时得到回复的请求终止掉，防止客户端一直等待
        for (Map.Entry<Long, Task> results : RESULTS.entrySet()) {
            Task result = results.getValue();
            result.setValue(
                    new RpcException().withStatus(RPC_CLIENT_STOP).withError("Client is shutting down")
            );
        }
    }

    public static <T> T getClient(Class<T> clazz) {
        if (clazz.isInterface()) {
            RpcClient rpcClient = clazz.getDeclaredAnnotation(RpcClient.class);
            if (rpcClient != null) {
                List<InetSocketAddress> addresses;
                String location = rpcClient.location();
                int serviceId = generateServiceId(rpcClient.service());
                if (location.startsWith(ZOOKEEPER_PREFIX)) {
                    addresses = obtainMetadataFromZookeeper(location, serviceId);
                } else {
                    addresses = Collections.singletonList(createInetAddress(location));
                }

                ClientProxy clientProxy = new ClientProxy(addresses, NETTY_CLIENT, serviceId);

                CLIENTS.put(serviceId, clientProxy);

                return CLIENT_GENERATOR.generate(clazz, serviceId, clientProxy);
            } else {
                throw new RpcException("Class is not a valid RpcClient, no @RpcClient annotated on class " + clazz);
            }
        }
        throw new RpcException("Class is not interface, @RpcClient only annotated on interface " + clazz);
    }

    private static int generateServiceId(String name) {
        int serviceId = Hashing.murmur3_32().hashString(name, UTF_8).asInt();
        while (serviceId < 0) {
            serviceId = Hashing.murmur3_32().hashString(name + serviceId, UTF_8).asInt();
        }
        return serviceId;
    }

    private static InetSocketAddress createInetAddress(String location) {
        location = location.substring(LOCAL.length());
        int index = location.lastIndexOf(":");
        if (index != -1) {
            return new InetSocketAddress(
                    getHostName(location.substring(0, index)), getPort(location.substring(index + 1))
            );
        } else {
            throw new RpcException("Invalid location address, valid address format hostname:port");
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
            throw new RpcException("Invalid port, valid port is 1024 - 65535", e);
        }
    }

    /**
     * zookeeper://127.0.0.1:2181/com.lee.rpc/services/{serviceId}/{127.0.0.1:8080}
     * 格式说明：每一个客服端都需要指定一个格式，也就是去那个地方去获取metadata的数据，通常情况下是客服端直接指定zookeeper://127.0.0.1:2181
     * 就可以了
     * 主格式如下：services下面有子节点
     * zookeeper://127.0.0.1:2181/com.lee.rpc/services/{serviceId}
     * --{serviceId} 每一个服务下面可能有很多个地址对应，表示有这么多个微服务提供了该serviceId的服务，客户端需要根据情况来获取这些服务
     * --127.0.0.1:8080
     * --127.0.0.1:8081
     * --127.0.0.1:8082
     * -- .....
     * --{serviceId} 之前说过，一个port可以对应多个RpcService，所以这个地方需要分组来注册，也就是同一个port端口号，可以提供不同的服务
     * --127.0.0.1:8081
     * --.....
     * --{serviceId}
     * --127.0.0.1:8080
     * ----
     *
     * @param location  需要访问的Zookeeper位置
     * @param serviceId 获取指定serviceId对应的所有微服务地址
     */
    private static List<InetSocketAddress> obtainMetadataFromZookeeper(String location, int serviceId) {
        //TODO:从Zookeeper里面去获取Metadata的信息
        //这个地方就已经实例化好了，也就是RpcHelper已经注册好了
        RpcHelper.registerClient(null, null);
        return null;
    }
}
