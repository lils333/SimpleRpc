package com.lee.rpc.helper.client;

import com.lee.rpc.util.CityHash;
import io.netty.channel.Channel;
import io.netty.channel.ChannelId;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import static com.lee.rpc.helper.RpcHelper.KEY;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * 一致性hash算法，构建一个hash环，每一次请求，都会根据request的ID，去获取Channel来发送消息，这个Channel采用虚拟Channel，也就是一个
 * 物理真实的Channel对应着10个相同的引用，也就是这个10个引用指向同一个Channel，这10个引用被分散在这个一致性hash环上面去提供服务
 * 采用该方式的主要原因是简单，而不是为了高效率，并且还支持动态的添加和移除Channel，当在集成zookeeper的时候，只需要直接添加就可以了
 */
@Slf4j
public class KetamaChannel {

    private static final ThreadLocal<ByteBuffer> BUFFER = ThreadLocal.withInitial(() -> ByteBuffer.allocate(8));
    private static final int VIRTUAL_NODE_COUNT = 10;

    private SortedMap<Long, Channel> channels = new TreeMap<>();

    public synchronized void fillChannelWithWeight(Channel channel) {
        SortedMap<Long, Channel> newChannels = new TreeMap<>(channels);
        if (channel != null) {
            List<Long> locations = new ArrayList<>(VIRTUAL_NODE_COUNT);
            for (int i = 0; i < VIRTUAL_NODE_COUNT; i++) {
                long mapping = mapping(channel.id(), i);
                newChannels.put(mapping, channel);
                //把自己的位置设置进去，方便删除的时候使用
                locations.add(mapping);
            }
            channels = newChannels;
            channel.attr(KEY).set(locations);
        }
    }

    public synchronized void removeChannelFrom(List<Long> locations) {
        SortedMap<Long, Channel> newChannels = new TreeMap<>(channels);
        for (Long location : locations) {
            newChannels.remove(location);
        }
        channels = newChannels;
    }

    public synchronized void replaceChannelFrom(Channel channel, List<Long> locations) {
        SortedMap<Long, Channel> newChannels = new TreeMap<>(channels);
        for (Long location : locations) {
            newChannels.replace(location, channel);
        }
        channels = newChannels;
    }

    public Channel getChannel(long requestId) {
        requestId = mapping(requestId);
        final Channel rv;

        //key的hash值没有和hash环上面的salt对应，这个时候就顺时针获取离该hash最近的salt
        if (!channels.containsKey(requestId)) {
            SortedMap<Long, Channel> tailMap = channels.tailMap(requestId);
            //如果计算出来的当前hash值在这个环里面是最大的，也就是没有大于该hash值的salt存在，那么这个时候就获取
            //hash环里面的最小的的那个salt
            if (tailMap.isEmpty()) {
                requestId = channels.firstKey();
            } else {
                requestId = tailMap.firstKey();
            }
        }

        //如果计算出来的hash刚好就是这个salt对应的Channel，那么直接返回
        rv = channels.get(requestId);
        return rv;
    }

    private long mapping(long key) {
        ByteBuffer byteBuffer = BUFFER.get();
        try {
            byteBuffer.putLong(key);
            byte[] array = byteBuffer.array();
            return CityHash.cityHash64WithSeed(array, 0, array.length, 9527);
        } finally {
            byteBuffer.clear();
        }
    }

    private long mapping(ChannelId id, int index) {
        byte[] bytes = (id.asLongText() + "_#_" + index).getBytes(UTF_8);
        return CityHash.cityHash64WithSeed(bytes, 0, bytes.length, 9527);
    }
}
