package com.lee.rpc.helper.recycler;

import com.lee.rpc.RpcRequest;
import io.netty.util.internal.ObjectPool;

/**
 * 获取的时候是在当前线程的ThreadLocal里面获取，释放的时候可以不在同一个线程里面，也就是当在一个线程创建对象以后，在其他线程释放
 * 是可以的，但是创建对象是在当前线程的ThreadLocal里面来进行的
 *
 * @author Administrator
 */
public class RpcRequestRecycler {

    private static final ObjectPool<RpcRequest> RECYCLER = ObjectPool.newPool(RpcRequest::new);

    private RpcRequestRecycler() {
    }

    public static RpcRequest newInstance(int serviceId, long requestId, byte methodId) {
        RpcRequest request = RECYCLER.get();
        return request.serviceId(serviceId).requestId(requestId).methodId(methodId);
    }

    public static void recycle(RpcRequest request) {
        request.recycle();
    }
}
