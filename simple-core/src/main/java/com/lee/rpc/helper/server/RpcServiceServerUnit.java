package com.lee.rpc.helper.server;

import com.lee.rpc.RpcMethodUnit;
import com.lee.rpc.RpcService;
import com.lee.rpc.helper.Weight;
import io.netty.util.collection.ByteObjectHashMap;
import io.netty.util.collection.ByteObjectMap;
import lombok.Data;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * 1，同一个address 可以对应多个不同的RpcService，这些RpcService可以是相同的serviceId或者是不同的serviceId，不同的serviceId会
 * merge成一个RpcService
 *
 * @author Administrator
 */
@Data
public class RpcServiceServerUnit {

    private int serviceId;
    private int workers;
    private Weight weight;
    private RpcService rpcService;
    private InetSocketAddress address;
    private ByteObjectMap<RpcMethodUnit> methodIdMapping = new ByteObjectHashMap<>(16);

    public RpcMethodUnit getMethodUnit(byte methodId) {
        return methodIdMapping.get(methodId);
    }

    /**
     * 同一个服务只需要merge就可以了，同一个服务他们提供的是相同的服务
     *
     * @param newRpcService 新的服务
     */
    public void merge(RpcService newRpcService) {
        rpcService.mergeFrom(newRpcService);
        this.methodIdMapping = rpcService.getAllRpcMethod();
    }

    public Set<ExecutorService> shutdown() {
        return rpcService.shutdown();
    }

    public Weight getWeight() {
        return weight;
    }

    public void setWeight(Weight weight) {
        this.weight = weight;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RpcServiceServerUnit that = (RpcServiceServerUnit) o;

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
    public String toString() {
        return "RpcServiceServerUnit{" +
                "serviceId=" + serviceId +
                ", workers=" + workers +
                ", weight=" + weight +
                ", rpcService=" + rpcService +
                ", address=" + address +
                ", methodIdMapping=" + methodIdMapping +
                '}';
    }
}
