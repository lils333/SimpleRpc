package com.lee.rpc.helper;

import com.lee.rpc.RpcException;
import com.lee.rpc.RpcRequest;
import com.lee.rpc.RpcService;
import com.lee.rpc.executor.DelayWorker;
import com.lee.rpc.helper.client.RpcServiceClientUnit;
import com.lee.rpc.helper.server.NettyServer;
import com.lee.rpc.helper.server.RpcServiceServerUnit;
import com.lee.rpc.util.SnowFlakeIdGenerator;
import io.netty.util.AttributeKey;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.lee.rpc.util.Constant.ABNORMAL;
import static com.lee.rpc.util.SnowFlakeIdGenerator.START_TIME;
import static com.lee.rpc.util.exception.ErrorType.RPC_SERVER_STOP;

/**
 * @author Administrator
 */
@Slf4j
public class RpcHelper {

    public static final AttributeKey<List<Long>> KEY = AttributeKey.newInstance("KEY");
    public static final AttributeKey<InetSocketAddress> ADDRESS = AttributeKey.newInstance("ADDRESS");
    public static final AttributeKey<String> ZOOKEEPER = AttributeKey.newInstance("ZOOKEEPER");
    public static final String ZOOKEEPER_PREFIX = "zookeeper://";

    private RpcHelper() {
    }

    private static final IntObjectMap<RpcServiceServerUnit> SERVER_RPC_SERVICES = new IntObjectHashMap<>(4);
    private static final IntObjectMap<RpcServiceClientUnit> CLIENT_RPC_SERVICES = new IntObjectHashMap<>(4);

    private static NettyServer nettyServer;

    private static long workerId;
    private static long startTime = START_TIME;

    public synchronized static SnowFlakeIdGenerator createIdGenerator() {
        if (workerId > 1023) {
            workerId = 1;
            startTime = START_TIME - 123456789L;
        } else {
            workerId++;
        }
        return new SnowFlakeIdGenerator(workerId, startTime);
    }

    /**
     * 服务端使用
     * 同一个address可以存在多个不同的RpcService，相同的serviceId会merge, 同一个serviceId不能够发布到多个不同的address
     *
     * @param address    发布的端口号
     * @param rpcService 当前端口对应的RpcServer信息
     */
    public static synchronized void registerServer(InetSocketAddress address, RpcService rpcService) {
        RpcServiceServerUnit unit = SERVER_RPC_SERVICES.get(rpcService.getServiceId());
        if (unit == null) {
            unit = new RpcServiceServerUnit();
            unit.setServiceId(rpcService.getServiceId());
            unit.setWorkers(rpcService.getWorkers());
            unit.setAddress(address);
            unit.setWeight(rpcService.getWeight());
            unit.setRpcService(rpcService);
            unit.setMethodIdMapping(rpcService.getAllRpcMethod());
            SERVER_RPC_SERVICES.put(rpcService.getServiceId(), unit);
        } else {
            if (!address.equals(unit.getAddress())) {
                throw new RpcException("Can not publish same RpcService to multiple addresses");
            }

            //同一个服务里面只要有一个写明需要发布到Zookeeper上面去，那么就需要把该服务发布到Zookeeper上面去
            if (rpcService.getLocation().startsWith(RpcHelper.ZOOKEEPER_PREFIX)) {
                unit.getRpcService().setLocation(rpcService.getLocation());
            }

            //同一个serviceId提供的所有服务，需要merge在一起
            unit.merge(rpcService);
        }
    }

    /**
     * 根据serviceId来获取提供的服务，如果返回为null，表示该app没有提供serverId对应的服务
     * 一个服务只能够发布到一个address上面去
     *
     * @param serviceId 当前的请求，主要是获取serviceId
     * @return 返回该app提供的服务
     */
    public static RpcServiceServerUnit getRpcServiceUnit(int serviceId) {
        return SERVER_RPC_SERVICES.get(serviceId);
    }

    public static IntObjectMap<RpcServiceServerUnit> getRpcServiceUnits() {
        return SERVER_RPC_SERVICES;
    }

    /**
     * 客服端不需要port对应关系，port对应关系只是给服务器返回metadata给客服端使用的
     * 客服端只是需要保存从服务器那边传递过来的metadata而已,也就是只需要保存从Zookeeper上面获取过来的Metadata而已
     * 注意同一个serviceId提供的服务一定是一致的，所以这个地方只需要一个Metadata就可以了，不需要多个
     *
     * @param address    提供服务的地址
     * @param rpcService 提供服务的RpcService
     */
    public static synchronized void registerClient(InetSocketAddress address, RpcService rpcService) {
        RpcServiceClientUnit unit = CLIENT_RPC_SERVICES.get(rpcService.getServiceId());
        if (unit == null) {
            unit = new RpcServiceClientUnit();
            unit.setServiceId(rpcService.getServiceId());
            unit.addAddress(address, rpcService);
            unit.setMethodIdMapping(rpcService.getAllRpcMethod());
            CLIENT_RPC_SERVICES.put(rpcService.getServiceId(), unit);
        } else {
            //同一个RpcService服务里面的所有的方法都是一样的，也就是同一个serviceId对应的所有的方法都是一致的
            //唯一不同的就是发布到不同的地方，也就是多个地方同时提供服务，所以这个地方只是添加提供服务的address
            unit.addAddress(address, rpcService);
        }
    }

    public static RpcServiceClientUnit getRpcClientUnit(int serviceId) {
        return CLIENT_RPC_SERVICES.get(serviceId);
    }

    public static boolean isClientReady(int serviceId) {
        return CLIENT_RPC_SERVICES.containsKey(serviceId);
    }

    public static synchronized void startRpcService() {
        if (nettyServer == null) {
            nettyServer = new NettyServer();
        } else {
            return;
        }

        //如果需要发布到zookeeper上面去，那么先把metadata发布到Zookeeper上面去
        for (IntObjectMap.PrimitiveEntry<RpcServiceServerUnit> next : SERVER_RPC_SERVICES.entries()) {
            RpcServiceServerUnit serverUnit = next.value();
            InetSocketAddress address = serverUnit.getAddress();
            String location = serverUnit.getRpcService().getLocation();
            if (location.startsWith(ZOOKEEPER_PREFIX)) {
                publishMetadataToZookeeper(serverUnit);
            }
            nettyServer.bind(address, serverUnit);
        }
    }

    public static synchronized void stopRpcService() {
        if (nettyServer != null) {
            //停止bossWorker，不要在接受新的请求
            nettyServer.stopAcceptor();
        }

        //通知DelayWorker把延迟执行队列里面的任务，终止掉
        DelayWorker.getInstance().close();

        //停止共享和非共享业务线程池，让线程池里面的任务全部得到处理, 并且不再接受新的RpcRequest
        for (IntObjectMap.PrimitiveEntry<RpcServiceServerUnit> next : SERVER_RPC_SERVICES.entries()) {
            RpcServiceServerUnit serverUnit = next.value();
            Set<ExecutorService> executors = serverUnit.shutdown();
            for (ExecutorService executor : executors) {
                stopExecutor(executor);
            }
        }

        if (nettyServer != null) {
            //停止bossWorker，不再接受新的请求，并且关闭Channel
            nettyServer.stopWorker();
        }
    }

    private static void stopExecutor(ExecutorService executor) {
        try {
            //每一个线程池如果10秒钟没有处理完成队列里面的RpcRequest，那么直接把么有处理的请求发送回客服端
            if (executor.awaitTermination(10, TimeUnit.SECONDS)) {
                log.info("Executor {} is stopped", executor);
            } else {
                //把当前Executor队列里面的还没有处理的RpcRequest发送回客服端，告诉客服端，失败原因
                List<Runnable> tasks = executor.shutdownNow();
                for (Runnable task : tasks) {
                    RpcRequest request = (RpcRequest) task;
                    request.getChannel().writeAndFlush(
                            request.type(ABNORMAL).body(
                                    new RpcException()
                                            .withStatus(RPC_SERVER_STOP)
                                            .withError("RpcService is shutting down")
                            )
                    );
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted by any other thread when this Executor is shutting down", e);
        }
    }

    /**
     * 这个地方是发布Metadata的地方，也就是把当前的Metadata发布到Zookeeper上面去，注意发布的时候需要添加上address
     * 也就是需要发布到Zookeeper的哪一个节点上面去，方便客服端负载均衡的时候使用
     *
     * @param serverUnit 需要发布的服务
     */
    private static void publishMetadataToZookeeper(RpcServiceServerUnit serverUnit) {
        log.info(
                "Publish RpcService {} to Zookeeper successfully, metadata {}", serverUnit.getServiceId(), serverUnit
        );
    }
}
