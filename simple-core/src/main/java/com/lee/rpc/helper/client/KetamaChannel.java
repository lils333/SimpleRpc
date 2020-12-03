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
 * 一致性hash算法，构建一个hash环，每一次请求，都会根据request的ID，去获取具体需要发送请求的位置
 * 主要是为了方便动态的添加提供服务的节点而是用，虽然性能不是最好的，但是实现不叫简单
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

    public Channel getChannel(long channelId) {
        channelId = mapping(channelId);
        final Channel rv;
        //key的hash值没有和hash环上面的salt对应，这个时候就顺时针获取离该hash最近的salt
        if (!channels.containsKey(channelId)) {
            SortedMap<Long, Channel> tailMap = channels.tailMap(channelId);
            //如果计算出来的当前hash值在这个环里面是最大的，也就是没有大于该hash值的salt存在，那么这个时候就获取
            //hash环里面的最小的的那个salt
            if (tailMap.isEmpty()) {
                channelId = channels.firstKey();
            } else {
                channelId = tailMap.firstKey();
            }
        }
        //如果计算出来的hash刚好就是这个salt对应的Channel，那么直接返回
        rv = channels.get(channelId);
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
