package com.lee.rpc.executor;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lee.rpc.RpcException;
import com.lee.rpc.RpcRequest;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFutureListener;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.DelayQueue;
import java.util.concurrent.ThreadFactory;

import static com.lee.rpc.util.Constant.ABNORMAL;
import static com.lee.rpc.util.exception.ErrorType.RPC_SERVER_STOP;
import static com.lee.rpc.util.exception.ErrorType.SERVICE_BUSY;

/**
 * 全局共享一个延迟队列就可以了，因为Channel的数量不会太多，每一个Channel实际上都对应着一个Socket
 */
@Slf4j
public class DelayWorker implements Runnable {

    private static DelayWorker delayWorker;

    private final DelayQueue<RpcRequest> delayChannels = new DelayQueue<>();
    private final Thread worker;

    private boolean isShutdown;

    public static synchronized DelayWorker getInstance() {
        if (delayWorker == null) {
            delayWorker = new DelayWorker();
        }
        return delayWorker;
    }

    private DelayWorker() {
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setDaemon(false)
                .setNameFormat("Delay-Request-%d")
                .build();
        worker = threadFactory.newThread(this);
        worker.start();
    }

    private void active(Channel channel) {
        ChannelConfig config = channel.config();
        if (!config.isAutoRead()) {
            config.setAutoRead(true);
            channel.read();
        }
    }

    private void inActive(Channel channel) {
        ChannelConfig config = channel.config();
        if (config.isAutoRead()) {
            config.setAutoRead(false);
        }
    }

    public void add(RpcRequest request) {
        Channel channel = request.getChannel();
        if (!isShutdown) {
            if (request.getRetryCount() >= 4) {
                channel.writeAndFlush(
                        request.type(ABNORMAL).body(
                                new RpcException()
                                        .withStatus(SERVICE_BUSY)
                                        .withError("RpcService method executor is busy, please try again later")
                        )
                ).addListener((ChannelFutureListener) future -> {
                    if (future.isSuccess()) {
                        active(channel);
                    } else {
                        log.error("Can not send service busy msg to client", future.cause());
                    }
                });
            } else {
                request.setWaitTime(System.currentTimeMillis() + 5000);
                request.setRetryCount(request.getRetryCount() + 1);
                delayChannels.offer(request);
                inActive(channel);
            }
        } else {
            channel.writeAndFlush(
                    request.type(ABNORMAL).body(
                            new RpcException().withStatus(RPC_SERVER_STOP).withError("RpcService is shutting down")
                    )
            );
            inActive(channel);
        }
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                RpcRequest request = delayChannels.take();
                if (request.getRetryCount() >= 4) {
                    final Channel channel = request.getChannel();
                    channel.writeAndFlush(
                            request.type(ABNORMAL).body(
                                    new RpcException()
                                            .withStatus(SERVICE_BUSY)
                                            .withError("RpcService method executor is busy, please try again later")
                            )
                    ).addListener((ChannelFutureListener) future -> {
                        //一旦发送成功了以后，那么就把当前Channel的可读事件重新注册上去
                        if (future.isSuccess()) {
                            active(channel);
                        }
                    });
                } else {
                    request.getMethodUnit().getExecutor().execute(request);
                }
            } catch (RpcException e) {
                RpcRequest rpcRequest = e.getRpcRequest();
                rpcRequest.getChannel().writeAndFlush(rpcRequest.type(ABNORMAL).body(e))
                        .addListener((ChannelFutureListener) future -> {
                            if (e.getStatus() != RPC_SERVER_STOP) {
                                active(future.channel());
                            }
                            if (!future.isSuccess()) {
                                log.error("Can not send exception to client", future.cause());
                            }
                        });
                log.error("RpcException happen", e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupt by any other thread", e);
            } catch (Exception e) {
                log.warn("Unexpected exception", e);
            }
        }

        RpcRequest[] rpcRequests = delayChannels.toArray(new RpcRequest[]{});
        for (RpcRequest rpcRequest : rpcRequests) {
            rpcRequest.getChannel()
                    .writeAndFlush(
                            rpcRequest.type(ABNORMAL).body(
                                    new RpcException()
                                            .withStatus(RPC_SERVER_STOP)
                                            .withError("RpcService is shutting down")
                            )
                    );
        }
    }

    public synchronized void close() {
        if (!this.isShutdown) {
            this.isShutdown = true;
            if (worker != null) {
                worker.interrupt();
            }
        }
    }
}
