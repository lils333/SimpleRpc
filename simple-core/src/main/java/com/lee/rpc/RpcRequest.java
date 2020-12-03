package com.lee.rpc;

import io.netty.channel.Channel;
import io.netty.util.internal.ObjectPool;
import lombok.Data;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import static com.lee.rpc.util.Constant.*;
import static com.lee.rpc.util.exception.ErrorType.SERVER_ERROR;

/**
 * @author Administrator
 */
@Data
public class RpcRequest implements Serializable, Runnable, Delayed {

    private final ObjectPool.Handle<RpcRequest> handle;
    private int serviceId;
    private long requestId;
    private byte methodId;
    private byte typeId;

    private transient Object body;
    private transient RpcMethodUnit methodUnit;
    private transient Channel channel;
    private transient long waitTime;
    private transient int retryCount;

    public RpcRequest(ObjectPool.Handle<RpcRequest> handle) {
        this.handle = handle;
    }

    public RpcRequest requestId(long requestId) {
        this.requestId = requestId;
        return this;
    }

    public RpcRequest serviceId(int serviceId) {
        this.serviceId = serviceId;
        return this;
    }

    public RpcRequest methodId(byte methodId) {
        this.methodId = methodId;
        return this;
    }

    public RpcRequest channel(Channel channel) {
        this.channel = channel;
        return this;
    }

    public RpcRequest type(byte typeId) {
        this.typeId = typeId;
        return this;
    }

    public RpcRequest withMethodUnit(RpcMethodUnit methodUnit) {
        this.methodUnit = methodUnit;
        return this;
    }

    public RpcRequest body(Object object) {
        this.body = object;
        this.typeId = object == null ? EMPTY_TYPE : NORMAL;
        return this;
    }

    public void recycle() {
        this.body = null;
        this.methodUnit = null;
        this.channel = null;
        handle.recycle(this);
    }

    @Override
    public void run() {
        try {
            getChannel().writeAndFlush(body(methodUnit.invoke(getBody())));
        } catch (RpcException e) {
            getChannel().writeAndFlush(type(ABNORMAL).body(e));
        } catch (Exception e) {
            getChannel().writeAndFlush(type(ABNORMAL).body(
                    new RpcException(e).withStatus(SERVER_ERROR).withError(e.getMessage()))
            );
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RpcRequest that = (RpcRequest) o;

        return new EqualsBuilder()
                .append(requestId, that.requestId)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(requestId)
                .toHashCode();
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(this.waitTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        return (int) (this.getDelay(TimeUnit.MILLISECONDS) - o.getDelay(TimeUnit.MILLISECONDS));
    }

    public RpcRequest withRetryCount(int count) {
        retryCount = count;
        return this;
    }
}
