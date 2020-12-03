package com.lee.rpc.helper.client;

import com.lee.rpc.RpcMethodUnit;
import com.lee.rpc.RpcService;
import com.lee.rpc.helper.Weight;
import io.netty.util.collection.ByteObjectHashMap;
import io.netty.util.collection.ByteObjectMap;
import lombok.Data;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * 封装类，主要是为了表示当前serviceId对应的所有方法的信息，之所以需要封装一下，是因为不同的serviceId里面可能存在相同ID
 * 所以需要封装一下
 *
 * @author Administrator
 */
@Data
public class RpcServiceClientUnit {
    private int serviceId;

    /**
     * 虽然都是同一个服务，但是权重和发布的地方是不一样的
     */
    private Map<InetSocketAddress, RpcService> addresses = new HashMap<>();

    /**
     * 虽然在不同地方提供服务，但是他们所提供的服务类型，也就是方法是一样的
     */
    private ByteObjectMap<RpcMethodUnit> methodIdMapping = new ByteObjectHashMap<>();

    public void add(Byte methodId, RpcMethodUnit methodUnit) {
        methodIdMapping.put(methodId, methodUnit);
    }

    public RpcMethodUnit getMethodUnit(byte methodId) {
        return methodIdMapping.get(methodId);
    }

    public void addAddress(InetSocketAddress address, RpcService rpcService) {
        addresses.put(address, rpcService);
    }

    public Weight getWeight(InetSocketAddress address) {
        return addresses.get(address).getWeight();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RpcServiceClientUnit that = (RpcServiceClientUnit) o;

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
}
