package com.lee.rpc.helper.client;

import com.lee.rpc.RpcException;
import com.lee.rpc.RpcMethodUnit;
import com.lee.rpc.RpcRequest;
import com.lee.rpc.helper.RpcHelper;
import com.lee.rpc.helper.Weight;
import com.lee.rpc.helper.client.ClientHelper.Task;
import com.lee.rpc.helper.recycler.RpcRequestRecycler;
import com.lee.rpc.util.SnowFlakeIdGenerator;
import com.lee.rpc.util.exception.ApplicationException;
import com.lee.rpc.util.exception.RetryException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.lee.rpc.helper.RpcHelper.*;
import static com.lee.rpc.helper.client.ClientHelper.putTask;
import static com.lee.rpc.helper.client.ClientHelper.removeTask;
import static com.lee.rpc.util.Constant.*;

/**
 * 每一个使用@RpcClient标注的类都需要指定访问的服务，也就是确定它需要访问那个服务，每一个服务只能够发布到一个端口上面去
 * 当然一个端口可以发布不同的服务，我们在获取metadata的时候，先要知道客服端需要访问的服务，只有知道了想要访问的服务
 * 才可以拿到对应的metadata，至于这个metadata是在Zookeeper还是在本地，客服端可以自己指定
 *
 * @author Administrator
 */
@Slf4j
public class ClientProxy {

    private static final ThreadLocal<SnowFlakeIdGenerator>
            ID_GENERATOR = ThreadLocal.withInitial(RpcHelper::createIdGenerator);
    private static final int RETRY_COUNT = 3;

    private final KetamaChannel ketamaChannel = new KetamaChannel();
    private final List<InetSocketAddress> addresses;
    private final int serviceId;
    private final NettyClient nettyClient;

    private boolean isShutdown;

    public ClientProxy(List<InetSocketAddress> addresses, NettyClient client, int serviceId) {
        this.addresses = addresses;
        this.nettyClient = client;
        this.serviceId = serviceId;
        prepareChannels(client);
    }

    /**
     * 这个调用是我们使用javassist生成的类来调用的，不是显示自己调用的
     *
     * @param unit      需要调用那个方法
     * @param parameter 调用方法的参数
     * @return 返回调用后的结果
     */
    public Object invoke(RpcMethodUnit unit, Object parameter) {
        Object result;
        RpcRequest request = createRpcRequest(unit, parameter);
        while (true) {
            if (isShutdown) {
                throw new RpcException("Client is stopping, can not accept any Request");
            } else {
                result = sendRequest(request);
                if (result != request) {
                    break;
                }

                try {
                    TimeUnit.MILLISECONDS.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RpcException("Interrupt by any other thread, break send logic", e);
                }
            }
            request.requestId(ID_GENERATOR.get().generatorKey());
        }
        return result;
    }

    private Object sendRequest(RpcRequest request) {
        Channel channel = ketamaChannel.getChannel(request.getRequestId());
        if (channel.isWritable()) {
            try {
                Task task = putTask(request.getRequestId());
                channel.writeAndFlush(request);
                return task.get(30, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                //注意：这个地方可能会导致消息重复发送的问题，因为服务端可能已经在处理该消息了
                //只是客户端链接断开了而已,特别注意更新和插入的动作可能会导致状态发生变化，
                //如果重复了，那么可能会导致一些不可预期的行为，框架部分代码只能够保证超时重试
                //由服务设计者来决定幂等性
                removeTask(request.getRequestId());
                if (channel.isActive()) {
                    if (request.getRetryCount() > RETRY_COUNT) {
                        throw new RetryException(
                                "Can not send request to server with " + request.getRetryCount() + " retry", e
                        );
                    } else {
                        //最大的重试次数一定，那么这个地方就需要把重试次数+1，然后选择另外一个RpcServer去执行
                        return request.withRetryCount(request.getRetryCount() + 1);
                    }
                } else {
                    //发送该Request的Channel已经挂掉，那么需要重新选择一个Channel去执行
                    //但是已经执行的次数使用上一次的值，也就是超时的话，保证重试次数是一致的
                    //但是由于选择了另外一个RpcServer去执行，所以可能会导致多个RpcServer执行同一个数据的行为
                    //也就是在超时的时候选择另外一个RpcServer去执行的时候，可能会导致之前的数据还是在执行的情况，那么保证微服务的
                    //幂等性就是需要我们考虑的了，对于查询来说，没有问题，查询天然就是幂等性的
                    return request.withRetryCount(request.getRetryCount() + 1);
                }
            } catch (ExecutionException e) {
                removeTask(request.getRequestId());
                Throwable cause = e.getCause();
                if (cause instanceof RpcException) {
                    RpcException exception = (RpcException) cause;
                    switch (exception.getStatus()) {
                        case SERVICE_BUSY:
                            log.info("Service busy, send to another com.lee.rpc service {}", e.getMessage());
                            return request;
                        case SERVER_ERROR:
                        case SERIALIZER_ERROR:
                        case NOT_SUPPORT_TYPE:
                        case CLIENT_SERIALIZER_ERROR:
                        case RPC_SERVER_STOP:
                        case RPC_CLIENT_STOP:
                        case NOT_EXIST_SERVICE_ID:
                        case CLIENT_ERROR:
                            throw new ApplicationException(cause);
                        default:
                            throw new ApplicationException("Can not support ErrorType " + exception.getStatus(), cause);
                    }
                }
                throw new ApplicationException("Unexpected exception happened", cause);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                removeTask(request.getRequestId());
                throw new ApplicationException("Interrupted by any other thread, may be client is stopping", e);
            } catch (Exception e) {
                removeTask(request.getRequestId());
                throw new ApplicationException("Unexpected exception happened", e);
            }
        } else {
            //换一个Channel继续执行,因为当前Channel的writeBuffer已经满了，或者当前Channel根本就可以用
            return request;
        }
    }

    private RpcRequest createRpcRequest(RpcMethodUnit methodUnit, Object parameter) {
        //这个地方是在当前线程创建，但是在其他IO线程来释放
        RpcRequest request = RpcRequestRecycler.newInstance(
                serviceId, ID_GENERATOR.get().generatorKey(), methodUnit.getMethodId()
        );

        request.setMethodUnit(methodUnit);

        //现目前只支持一个参数，多个参数感觉没有啥子必要，后面如果需要在做
        request.body(parameter);

        request.setTypeId(parameter == null ? EMPTY_TYPE : OBJECT);

        return request;
    }

    private void prepareChannels(NettyClient nettyClient) {
        try {
            initKetamaChannel();
        } catch (Exception e) {
            nettyClient.shutdown();
            throw e;
        }
    }

    private void initKetamaChannel() {
        if (!RpcHelper.isClientReady(serviceId)) {
            //基于本地协议的就只有一个
            try {
                createChannelInternal(addresses.get(0), null).sync();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Interrupted by any other thread", e);
                throw new RpcException("Can not connect to address " + addresses.get(0), e);
            }
        } else {
            //从已经注册的RpcServiceClientUnit里面去获取需要的weight信息
            RpcServiceClientUnit clientUnit = RpcHelper.getRpcClientUnit(serviceId);

            //有几个地方提供服务
            for (InetSocketAddress address : addresses) {
                //每一个地方提供服务的权重
                Weight weight = clientUnit.getWeight(address);
                //按照权重来创建对应的channel
                for (int j = 0; j < weight.value(); j++) {
                    createChannelInternal(address, "zookeeper");
                }
            }
        }
    }

    private ChannelFuture createChannelInternal(final InetSocketAddress address, final String zookeeper) {
        return nettyClient.connect(address, this, serviceId, future -> {
            Channel channel = future.channel();
            if (future.isSuccess()) {
                channel.attr(ADDRESS).set(address);
                if (!RpcHelper.isClientReady(serviceId)) {
                    ByteBuf metadata = Unpooled.buffer(16)
                            .writeInt(serviceId)
                            .writeLong(METADATA)
                            .writeByte(METADATA)
                            .writeByte(METADATA)
                            .writeShort(EMPTY_VALUE);
                    channel.writeAndFlush(metadata);
                } else {
                    if (zookeeper != null) {
                        channel.attr(ZOOKEEPER).set(zookeeper);
                    }
                }
                ketamaChannel.fillChannelWithWeight(channel);
            } else {
                //如果不可用的话，那么添加一个延迟任务，每分钟发起对该address的连接请求，直到建立了连接为止
                if (zookeeper == null) {
                    //哪怕不成功，也可以把这个Channel放进去，只是这个时候的Channel是不可以写的，发送的时候只会导致不停的在获取Channel
                    ketamaChannel.fillChannelWithWeight(channel);

                    //之后再添加一个一直重试的调度任务，每分钟都去检查一下对应的address是否可用
                    channel.eventLoop().schedule(
                            new ReconnectAddress(address, null, channel.attr(KEY).get()), 1, TimeUnit.MINUTES
                    );
                } else {
                    //因为在每次建立连接的时候，实际上都已经重试了很多次了，如果重试了这么多次都还不能够建立连接，那么默认
                    //该zookeeper上面的对应的地址，可能已经不在提供正常服务了，这个时候，没有必要继续建立连接
                    //如果该服务后面再启动起来，那么根据动态发现原则，还是会动态把该地址添加进去的，所以这个地方不需要持续的建立连接
                    log.error("Can not connect to address from zookeeper " + address, future.cause());
                }
            }
        });
    }

    public void addChannel(final Channel channel, final List<Long> locations) {
        if (channel.isActive()) {
            channel.attr(KEY).set(locations);
            ketamaChannel.replaceChannelFrom(channel, locations);
        } else {
            //这个地方是给local使用的，也就是local还需要去建立连接, 如果是local的话，那么这个地方还是使用之前的那个不可用的Channel
            ReconnectAddress reconnectAddress = new ReconnectAddress(channel.attr(ADDRESS).get(), null);
            reconnectAddress.setLocations(locations);
            channel.eventLoop().schedule(
                    reconnectAddress, 1L, TimeUnit.SECONDS
            );
        }
    }

    public boolean isShutdown() {
        return isShutdown;
    }

    public void setShutdown(boolean shutdown) {
        isShutdown = shutdown;
    }

    public int getServiceId() {
        return this.serviceId;
    }

    public void createChannel(final InetSocketAddress address) {
        nettyClient.connect(address, this, serviceId, future -> {
            Channel channel = future.channel();
            if (future.isSuccess()) {
                channel.attr(ADDRESS).set(address);
                ketamaChannel.fillChannelWithWeight(channel);
            } else {
                log.error("Can not connect to address " + address, future.cause());
            }
        });
    }

    public void removeChannelFromKetama(List<Long> locations) {
        ketamaChannel.removeChannelFrom(locations);
    }

    class ReconnectAddress implements Runnable {

        private final InetSocketAddress address;
        private final String zookeeper;
        private List<Long> locations;

        public ReconnectAddress(InetSocketAddress address, String zookeeper) {
            this.address = address;
            this.zookeeper = zookeeper;
        }

        public ReconnectAddress(InetSocketAddress address, String zookeeper, List<Long> locations) {
            this.address = address;
            this.zookeeper = zookeeper;
            this.locations = locations;
        }

        @Override
        public void run() {
            //检查当前连接是否已经可以使用了, 如果已经可以使用了，那么恢复连接
            nettyClient.connect(address, ClientProxy.this, serviceId, future -> {
                Channel newChannel = future.channel();
                if (future.isSuccess()) {
                    newChannel.attr(ADDRESS).set(address);

                    if (zookeeper != null) {
                        newChannel.attr(ZOOKEEPER).set(zookeeper);
                    }

                    if (locations == null) {
                        //这个地方是在创建的时候，直接就失败了，后面根本就还没有location
                        ketamaChannel.fillChannelWithWeight(newChannel);
                    } else {
                        //如果location不为空，那么说明是已经连接上了，后面重新建立的连接
                        newChannel.attr(KEY).set(locations);
                        ketamaChannel.replaceChannelFrom(newChannel, locations);
                    }
                } else {
                    log.warn("Can not reconnect to server, retry reconnect again later", future.cause());

                    //2分钟以后，链接还是没有恢复，那么继续尝试建立连接
                    newChannel.eventLoop().schedule(this, 1, TimeUnit.MINUTES);
                }
            });
        }

        public void setLocations(List<Long> locations) {
            this.locations = locations;
        }
    }
}
