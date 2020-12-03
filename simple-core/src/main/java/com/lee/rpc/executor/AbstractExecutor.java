package com.lee.rpc.executor;

import com.lee.rpc.RpcException;
import com.lee.rpc.RpcRequest;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.lee.rpc.util.exception.ErrorType.RPC_SERVER_STOP;
import static com.lee.rpc.util.exception.ErrorType.SERVER_ERROR;

/**
 * @author Administrator
 */
@Slf4j
public abstract class AbstractExecutor implements Executor {

    protected final ConcurrentLinkedQueue<Channel> deniedChannels = new ConcurrentLinkedQueue<>();
    protected ThreadFactory threadFactory;
    protected Executor executor;
    protected OverflowMode overflowMode;
    protected int capacity;

    protected boolean isShutdown;

    public AbstractExecutor withThreadFactory(ThreadFactory threadFactory) {
        this.threadFactory = threadFactory;
        executor = createExecutor();
        return this;
    }

    public AbstractExecutor withOverflowMode(OverflowMode overflowMode) {
        this.overflowMode = overflowMode;
        return this;
    }

    public AbstractExecutor withCapacity(int capacity) {
        this.capacity = capacity;
        return this;
    }

    public Executor getInternalExecutor() {
        return this.executor;
    }

    @Override
    public void execute(Runnable task) {
        try {
            executor.execute(task);
        } catch (IllegalStateException e) {
            RpcRequest request = (RpcRequest) task;
            Channel channel = request.getChannel();
            ChannelConfig config = channel.config();
            if (config.isAutoRead()) {
                config.setAutoRead(false);
            }
            throw new RpcException(e)
                    .withStatus(RPC_SERVER_STOP).withError("RpcService is shutting down").withRequest((RpcRequest) task);
        } catch (RejectedExecutionException e) {
            DelayWorker.getInstance().add((RpcRequest) task);
        } catch (Exception e) {
            throw new RpcException(e)
                    .withStatus(SERVER_ERROR).withError(e.getMessage()).withRequest((RpcRequest) task);
        }
    }

    public void stop() {
        if (!isShutdown) {
            isShutdown = true;
            //要求底层是一个ExecutorService的实现，如果不是的话，就没有shutdown方法，不好调用
            //子类在实现的时候可以使用AbstractExecutorService去做适配
            if (executor instanceof ExecutorService) {
                ((ExecutorService) executor).shutdown();
            }
        }
    }

    protected BlockingQueue<Runnable> getBlockingQueue() {
        switch (overflowMode) {
            case DELAY:
                return new DelayWorkQueue(capacity);
            case WATER_MARK:
                return new WaterMarkQueue(capacity);
            default:
                throw new IllegalArgumentException("Can not support mode " + overflowMode);
        }
    }

    protected void inActiveChannel(Runnable task) {
        RpcRequest request = (RpcRequest) task;
        Channel channel = request.getChannel();
        ChannelConfig config = channel.config();
        if (config.isAutoRead()) {
            config.setAutoRead(false);
        }
        deniedChannels.add(channel);
    }

    /**
     * 线程安全的操作，主要目的是为了把那些没有注册READ事件的Channel重新注册上去
     */
    protected void activeChannel() {
        if (!isShutdown) {
            Channel channel = deniedChannels.poll();
            while (channel != null) {
                ChannelConfig config = channel.config();
                if (!config.isAutoRead()) {
                    config.setAutoRead(true);
                    channel.read();
                }
                channel = deniedChannels.poll();
            }
        }
    }

    /**
     * 由子类去决定到底使用什么样子的执行器，比如可以是单线程的，可以是caller调用线程执行，还可以是线程池
     * Executor最好是ExecutorService，这样好处理
     *
     * @return 返回一个执行器
     */
    protected abstract Executor createExecutor();

    /**
     * 延迟队列，也就是当发现队列已经满了以后，那么就不再消费当前Channel了, 并把当前的Channel设置成不可读，然后延迟5秒后，在继续添加任务
     */
    class DelayWorkQueue extends ArrayBlockingQueue<Runnable> {

        private DelayWorkQueue(int capacity) {
            super(capacity);
        }

        @Override
        public boolean offer(Runnable task) {
            if (!isShutdown) {
                return super.offer(task);
            }
            throw new IllegalStateException("RpcService is shutting down");
        }
    }

    /**
     * 带上了WaterMark的BlockingQueue，只要超过了high，那么说明处理不过来了，也就是说，需要控制客服端发送的速度
     */
    class WaterMarkQueue extends ArrayBlockingQueue<Runnable> {

        private final AtomicInteger sizeCounter = new AtomicInteger(0);
        private final int highWaterMark;
        private final int lowWaterMark;

        WaterMarkQueue(int capacity) {
            super(capacity);
            lowWaterMark = (int) (capacity * 0.55D);
            highWaterMark = (int) (capacity * 0.99D);
        }

        @Override
        public boolean offer(Runnable task) {
            if (!isShutdown) {
                boolean success = super.offer(task);
                if (sizeCounter.getAndIncrement() > highWaterMark) {
                    inActiveChannel(task);
                }
                return success;
            }
            throw new IllegalStateException("RpcService is shutting down");
        }

        @Override
        public Runnable poll() {
            Runnable task = super.poll();
            if (task == null || sizeCounter.decrementAndGet() <= lowWaterMark) {
                activeChannel();
            }
            return task;
        }

        @Override
        public Runnable poll(long timeout, TimeUnit unit) throws InterruptedException {
            Runnable task = super.poll(timeout, unit);
            if (task == null || sizeCounter.decrementAndGet() <= lowWaterMark) {
                activeChannel();
            }
            return task;
        }

        @Override
        public Runnable take() throws InterruptedException {
            Runnable task = super.take();
            if (sizeCounter.decrementAndGet() <= lowWaterMark) {
                activeChannel();
            }
            return task;
        }
    }
}
