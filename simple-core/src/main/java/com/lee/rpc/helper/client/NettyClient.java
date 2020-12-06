package com.lee.rpc.helper.client;

import com.lee.rpc.RpcException;
import com.lee.rpc.decoder.ClientTimeOutHandler;
import com.lee.rpc.decoder.RpcClientDecoder;
import com.lee.rpc.encoder.RpcClientEncoder;
import com.lee.rpc.helper.RpcHelper;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.lee.rpc.helper.RpcHelper.ADDRESS;
import static com.lee.rpc.helper.RpcHelper.KEY;
import static com.lee.rpc.helper.RpcHelper.ZOOKEEPER;
import static com.lee.rpc.helper.client.ClientHelper.CLIENT_GENERATOR;
import static java.lang.Thread.NORM_PRIORITY;

/**
 * 客户端JVM里面不管有多少个@RpcClient的实例，都使用一个Bootstrap来启动，只是创建的所有客服端的Channel都是在同一个EventLoopGroup
 * 里面
 *
 * @author l46li
 */
@Slf4j
class NettyClient {

    private static final int RETRY_COUNT = 10;

    private Bootstrap bootstrap;
    private EventLoopGroup worker;
    private boolean isShutdown;

    public void shutdown() {
        if (worker != null) {
            this.isShutdown = true;
            Future<?> future = worker.shutdownGracefully(3, 30, TimeUnit.SECONDS);
            try {
                future.sync();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Interrupted by any other thread", e);
            } catch (Exception e) {
                log.error("Can not stop client worker", e);
            }
        }
    }

    public ChannelFuture connect(InetSocketAddress address, ClientProxy clientProxy, int serviceId,
                                 ChannelFutureListener callBack) {
        return doConnect(
                address, clientProxy, new ConnectionListener(address, clientProxy, callBack, serviceId), serviceId
        );
    }

    private synchronized ChannelFuture doConnect(final InetSocketAddress address, final ClientProxy clientProxy,
                                                 ChannelFutureListener listener, int serviceId) {
        if (isShutdown) {
            throw new RpcException("Can not send connect request, client is stopping");
        }

        try {
            if (bootstrap == null) {
                bootstrap = getBootstrap();
            }

            //每次创建一个新的Handler
            bootstrap.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) {
                    ch.pipeline().addLast(
                            new FlushConsolidationHandler(
                                    20, true
                            )
                    );
                    ch.pipeline().addLast(
                            new IdleStateHandler(60, 0, 0)
                    );
                    ch.pipeline().addLast(new ReconnectHandler(address, clientProxy, serviceId));
                    ch.pipeline().addLast(new ClientTimeOutHandler(clientProxy));
                    ch.pipeline().addLast("decoder",
                            RpcHelper.isClientReady(serviceId) ?
                                    CLIENT_GENERATOR.createDecoder(address, serviceId, clientProxy) :
                                    new RpcClientDecoder(address, clientProxy)
                    );
                    ch.pipeline().addLast("encoder", new RpcClientEncoder());
                }
            });
            return bootstrap.connect(address).addListener(listener);
        } catch (Exception e) {
            throw new RpcException("Can not connect to server " + address, e);
        }
    }

    private Bootstrap getBootstrap() {
        return new Bootstrap().group(createEventLoopGroup())
                .channel(Epoll.isAvailable() ? EpollSocketChannel.class : NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_LINGER, 0)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000);
    }

    private EventLoopGroup createEventLoopGroup() {
        //注意：这个地方虽然指定了多少个线程去做操作
        //但并不是马上就会把线程创建出来，只有在当前线程上面发生事件，才会把线程创建出来，比如第一次写入一个消息，那么就会
        //触发创建IO线程，然后去执行这个WriteTask, 这个是整个客户端的JVM里面共享的，所以可以稍微大一点儿也没有关系
        int threads = Math.min(Runtime.getRuntime().availableProcessors() * 2, 32);

        BasicThreadFactory threadFactory = new BasicThreadFactory.Builder()
                .daemon(false)
                .namingPattern("Client-%d")
                .priority(NORM_PRIORITY)
                .build();

        if (Epoll.isAvailable()) {
            worker = new EpollEventLoopGroup(threads, threadFactory);
        } else {
            worker = new NioEventLoopGroup(threads, threadFactory);
        }

        return worker;
    }

    private class ConnectionListener implements ChannelFutureListener {
        private final InetSocketAddress address;
        private final ClientProxy clientProxy;
        private final ChannelFutureListener callback;
        private final int serviceId;

        private int retryNumber;

        public ConnectionListener(InetSocketAddress address,
                                  ClientProxy clientProxy, ChannelFutureListener listener, int serviceId) {
            this.address = address;
            this.clientProxy = clientProxy;
            this.callback = listener;
            this.serviceId = serviceId;
        }

        @Override
        public void operationComplete(ChannelFuture future) {
            if (!future.isSuccess()) {
                if (isShutdown) {
                    log.warn("No need to reconnect to server, client is stopping");
                } else {
                    if (++retryNumber < RETRY_COUNT) {
                        future.channel().eventLoop().schedule(() -> doConnect(
                                address, clientProxy, this, serviceId), 2L, TimeUnit.SECONDS
                        );
                    } else {
                        operation(future);
                    }
                }
            } else {
                operation(future);
            }
        }

        private void operation(ChannelFuture future) {
            try {
                callback.operationComplete(future);
            } catch (Exception e) {
                log.error("Unexpected exception happened", e);
            }
        }
    }

    private class ReconnectHandler extends ChannelInboundHandlerAdapter {
        private final InetSocketAddress address;
        private final ClientProxy clientProxy;
        private final int serviceId;

        public ReconnectHandler(InetSocketAddress address, ClientProxy clientProxy, int serviceId) {
            this.address = address;
            this.clientProxy = clientProxy;
            this.serviceId = serviceId;
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            final Channel channel = ctx.channel();
            if (!isShutdown) {
                //这个地方需要判断一下是否是zookeeper上面的，如果是zookeeper上面的话，那么可以不用一直去建立连接
                //因为zookeeper可能就是由于动态的在扩展，因为本身就是动态在扩展，所以没有必须再去建立连接，不过
                //为了防止闪断，还是需要重新去建立几次连接, 保证不是闪断的，如果重试了几次都还是失败，那么就认为是, 这已经被scale out了
                if (channel.hasAttr(ZOOKEEPER)) {
                    channel.eventLoop().schedule(() -> connect(address, clientProxy, serviceId,
                            new ChannelFutureListener() {
                                private final List<Long> locations = channel.attr(KEY).get();

                                @Override
                                public void operationComplete(ChannelFuture future) {
                                    if (future.isSuccess()) {
                                        future.channel().attr(ADDRESS).set(address);
                                        clientProxy.addChannel(future.channel(), locations);
                                    } else {
                                        //需要移除掉，该channel对应的位置
                                        clientProxy.removeChannelFromKetama(locations);
                                        log.error("Can not reconnect to zookeeper address" + address, future.cause());
                                    }
                                }
                            }), 1L, TimeUnit.SECONDS);
                } else {
                    //如果不是zookeeper模式，那么就是simple模式，这个时候需要一直尝试建立连接
                    channel.eventLoop().schedule(() -> connect(address, clientProxy, serviceId,
                            new ChannelFutureListener() {
                                private final List<Long> locations = channel.attr(KEY).get();

                                @Override
                                public void operationComplete(ChannelFuture future) {
                                    Channel newChannel = future.channel();
                                    newChannel.attr(ADDRESS).set(address);
                                    clientProxy.addChannel(
                                            newChannel.isActive() ? newChannel : channel, locations
                                    );
                                }
                            }), 1L, TimeUnit.SECONDS);
                }
            } else {
                log.info("Client is shutting down, no need to reconnect to server");
            }

            super.channelInactive(ctx);
        }
    }
}
