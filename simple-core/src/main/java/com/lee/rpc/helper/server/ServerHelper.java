package com.lee.rpc.helper.server;

import com.google.common.hash.Hashing;
import com.google.common.net.InetAddresses;
import com.lee.rpc.RpcException;
import com.lee.rpc.RpcService;
import com.lee.rpc.annotation.RpcMethod;
import com.lee.rpc.annotation.RpcServer;
import com.lee.rpc.helper.RpcHelper;
import io.netty.util.ResourceLeakDetector;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author Administrator
 */
@Slf4j
public class ServerHelper {

    public static final RpcServerGenerator SERVER_GENERATOR = new RpcServerGenerator();

    private static final List<RpcServerWrapper> RPC_SERVER_WRAPPERS = new ArrayList<>();

    static {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.DISABLED);
    }

    private ServerHelper() {
    }

    public static synchronized void startRpcService() {
        registerRpcService();
        RpcHelper.startRpcService();
    }

    public static synchronized void stopRpcService() {
        RpcHelper.stopRpcService();
    }

    public static void registerServer(Object instance) {
        HashSet<Class<?>> results = new HashSet<>();
        findRpcServer(instance.getClass(), results);
        if (results.isEmpty()) {
            throw new RpcException("@RpcServer must annotated on an interface");
        } else {
            for (Class<?> targetClass : results) {
                RpcServer rpcServer = targetClass.getDeclaredAnnotation(RpcServer.class);
                RpcServerWrapper rpcServerWrapper = new RpcServerWrapper();
                rpcServerWrapper.rpcServer = rpcServer;
                rpcServerWrapper.instance = instance;
                rpcServerWrapper.inter = targetClass;
                rpcServerWrapper.serviceId = generateServiceId(rpcServer.service());
                rpcServerWrapper.workers = rpcServer.works();
                RPC_SERVER_WRAPPERS.add(rpcServerWrapper);
            }
        }
    }

    /**
     * 获取所有@RpcServer的接口
     *
     * @param targetInterface 需要查找的类
     */
    private static void findRpcServer(Class<?> targetInterface, Set<Class<?>> results) {
        Class<?>[] interfaces = targetInterface.getInterfaces();
        for (Class<?> targetClass : interfaces) {
            RpcServer rpcServer = targetClass.getDeclaredAnnotation(RpcServer.class);
            if (rpcServer != null) {
                results.add(targetClass);
            } else {
                findRpcServer(targetClass, results);
            }
        }
    }

    private static void registerRpcService() {
        //同一个address，同一个serviceId，那么他们的RpcService里面的方法的ID可能会导致不一样，比如在不同的地方发布这个服务
        //那么可能存在不同的methodId，为了保证顺序一致性，那么还是需要
        RPC_SERVER_WRAPPERS.stream().sorted().forEach(
                rpcServerWrapper -> {
                    RpcService service = new RpcService();
                    service.setLocation(rpcServerWrapper.rpcServer.location());
                    service.setServiceId(rpcServerWrapper.serviceId);
                    service.setWorkers(rpcServerWrapper.workers);
                    service.setWeight(rpcServerWrapper.rpcServer.weight());
                    Method[] declaredMethods = rpcServerWrapper.inter.getDeclaredMethods();

                    //使用方法签名来排序，这样保证在其他地方这个顺序也是一致的
                    Arrays.sort(declaredMethods, Comparator.comparingInt(o -> generateServiceId(o.toString())));

                    for (Method declaredMethod : declaredMethods) {
                        service.register(
                                declaredMethod.getDeclaredAnnotation(RpcMethod.class),
                                declaredMethod,
                                rpcServerWrapper.instance,
                                rpcServerWrapper.inter
                        );
                    }
                    RpcHelper.registerServer(
                            createInetAddress(rpcServerWrapper.rpcServer.publish()), service
                    );
                }
        );
        RPC_SERVER_WRAPPERS.clear();
    }

    private static int generateServiceId(String serviceName) {
        int serviceId = Hashing.murmur3_32().hashString(serviceName, UTF_8).asInt();
        while (serviceId < 0) {
            serviceId = Hashing.murmur3_32().hashString(serviceName + serviceId, UTF_8).asInt();
        }
        return serviceId;
    }

    private static InetSocketAddress createInetAddress(String publish) {
        int index = publish.lastIndexOf(":");
        if (index != -1) {
            return new InetSocketAddress(
                    getHostName(publish.substring(0, index)), getPort(publish.substring(index + 1))
            );
        } else {
            throw new RpcException("Invalid publish address, valid address format hostname:port");
        }
    }

    private static InetAddress getHostName(String hostname) {
        try {
            return InetAddresses.forString(hostname);
        } catch (Exception e) {
            throw new RpcException("hostname is invalid", e);
        }
    }

    private static short getPort(String port) {
        try {
            return Short.parseShort(port);
        } catch (NumberFormatException e) {
            throw new RpcException("Invalid port, valid port valid is 1024 - 65535", e);
        }
    }

    private static class RpcServerWrapper implements Comparable<RpcServerWrapper> {
        int workers;
        Class<?> inter;
        RpcServer rpcServer;
        Object instance;
        int serviceId;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return true;
            }

            RpcServerWrapper that = (RpcServerWrapper) o;

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
        public int compareTo(RpcServerWrapper wrapper) {
            if (wrapper == null || this == wrapper) {
                return 1;
            }
            int i = this.serviceId - wrapper.serviceId;
            if (i == 0) {
                //默认情况下，就算同一个serviceId，同一个address, 他们的接口肯定是不一样的，也就是包级别下面的全限定类名，是不一样的
                //因为一样就会导致冲突的，所以这个地方直接使用接口的名字来进行排序比较，这样在每一个平台上面他们的顺序就一致了
                //就不存在方法名字不一样的情况了，因为只要保证了RpcService顺序一致，那么就保证了方法的顺序
                return generateServiceId(inter.getName()) - generateServiceId(wrapper.inter.getName());
            }
            return i;
        }

        @Override
        public String toString() {
            return "RpcServerWrapper{" +
                    "id=" + serviceId +
                    '}';
        }
    }
}
