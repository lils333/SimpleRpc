package com.lee.rpc.helper.server;

import com.lee.rpc.decoder.ServerTimeOutHandler;
import com.lee.rpc.encoder.RpcServerEncoder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static com.lee.rpc.helper.server.ServerHelper.SERVER_GENERATOR;

/**
 * @author l46li
 */
@Slf4j
public class NettyServer {

    private final ThreadFactory acceptorThreadFactory = new BasicThreadFactory.Builder()
            .daemon(false)
            .namingPattern("Acceptor-%d")
            .priority(Thread.NORM_PRIORITY)
            .build();

    private final ThreadFactory workerThreadFactory = new BasicThreadFactory.Builder()
            .daemon(false)
            .namingPattern("Worker-%d")
            .priority(Thread.NORM_PRIORITY)
            .build();

    private final Set<EventLoopGroup> bossWorkers = new HashSet<>();

    private final Map<InetSocketAddress, EventLoopGroup> publishAddresses = new HashMap<>();

    public void bind(final InetSocketAddress address, RpcServiceServerUnit serverUnit) {
        if (!publishAddresses.containsKey(address)) {
            ServerBootstrap bootstrap = new ServerBootstrap()
                    .group(createBossWorker(), createWorker(address, serverUnit.getWorkers()))
                    .channel(Epoll.isAvailable() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                    .childOption(ChannelOption.SO_REUSEADDR, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.SO_LINGER, 0)
                    .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, createWriteBufferWaterMark())
                    .childHandler(createChannel());

            bootstrap.bind(address).addListener(future -> {
                if (future.isSuccess()) {
                    log.info("Bind to {} successfully", address);
                } else {
                    log.warn("Can not bind to address " + address, future.cause());
                }
            });
        }
    }

    private ChannelHandler createChannel() {
        return new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
                ch.pipeline().addLast(
                        new FlushConsolidationHandler(30, true)
                );
                ch.pipeline().addLast(
                        new IdleStateHandler(0, 0, 200)
                );
                ch.pipeline().addLast(SERVER_GENERATOR.createRpcServerDecoder());
                ch.pipeline().addLast(new RpcServerEncoder());
                ch.pipeline().addLast(new ServerTimeOutHandler());
            }
        };
    }

    /**
     * 设置每一个Channel对应的ChannelOutboundBuffer的WriteBufferWaterMark，每一个Channel都有一个这个水位线来控制
     * 当前Channel应该写入的大小，注意这个只是提示，也就是只要ChannelOutboundBuffer里面的数据满了，那么就会修改isWritable
     * 为false，所以在写数据之前，可以判断一下这个HighWaterMark是否超过了最大大小
     *
     * @return 返回自定义的WriteBufferWaterMark
     */
    private WriteBufferWaterMark createWriteBufferWaterMark() {
        return new WriteBufferWaterMark(32 * 1024, 64 * 1024);
    }

    private EventLoopGroup createWorker(InetSocketAddress address, int workers) {
        EventLoopGroup worker = Epoll.isAvailable() ?
                new EpollEventLoopGroup(workers, workerThreadFactory) :
                new NioEventLoopGroup(workers, workerThreadFactory);
        this.publishAddresses.put(address, worker);
        return worker;
    }

    private EventLoopGroup createBossWorker() {
        EventLoopGroup bossWorker = Epoll.isAvailable() ?
                new EpollEventLoopGroup(1, acceptorThreadFactory) :
                new NioEventLoopGroup(1, acceptorThreadFactory);
        bossWorkers.add(bossWorker);
        return bossWorker;
    }

    public void stopAcceptor() {
        //这个主要是针对ServerSocketChannel的，也就是会先处理请求，然后当前循环里面的请求处理完成以后，
        //在来完成关闭动作，先关闭ServerSocketChannel，然后继续处理已经在队列里面的数据，其实ServerSocketChannel一般是没有Task的
        //所以，ServerSocketChannel关闭以后，基本当前这个bossWorker就不会再接受请求了
        close(bossWorkers);
    }

    public void stopWorker() {
        //主要针对SocketChannel而言,也是当前EventLoop的逻辑处理完成以后，在来关闭，首先关闭所有注册到EventLoop上面的SocketChannel
        //也就是这个时候不会在接受任何写入的请求, 所有的这些写入的请求都会失败，所以只是保证当前循环获取的task处理完成，下一个循环是不会再处理
        //了，所以，如果我们有单独的线程池去处理请求的话，想要线程池把任务都处理完成，那么就需要先关闭业务线程，才可以的
        close(publishAddresses.values());
    }

    protected void close(Collection<EventLoopGroup> workerLoopGroup) {
        if (workerLoopGroup != null) {
            for (EventLoopGroup eventExecutors : workerLoopGroup) {
                Future<?> future = eventExecutors.shutdownGracefully(3, 30, TimeUnit.SECONDS);
                try {
                    future.sync();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.error("Interrupted by any other thread", e);
                } catch (Exception e) {
                    log.error("Can not stop acceptor worker", e);
                }
            }
        }
    }
}
