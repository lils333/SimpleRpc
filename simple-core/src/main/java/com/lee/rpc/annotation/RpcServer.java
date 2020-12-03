package com.lee.rpc.annotation;

import com.lee.rpc.helper.Weight;

import java.lang.annotation.*;

/**
 * @author l46li
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RpcServer {

    /**
     * zookeeper://{127.0.0.1:8080}
     * simple://{127.0.0.1:8080}
     *
     * @return 返回一个地址，主要是为了存储发布的信息, 这个可选指定。服务发布的时候使用, 如果不指定，那么认为是simple模式
     */
    String location() default "simple://";

    /**
     * 显示的指定hostname:port，实际上可以只指定port，然后程序自己来获取正确的IP地址，不过这个获取IP地址的过程需要好好考虑一下
     * 1）如果/etc/hosts 里面，没有配置，那么返回为null，如下不符合IP的情况
     * a，没有配置hostname到IP的映射，那么InetAddress.getLocalHost()返回为null
     * b，获取出来的是回环的地址，比如127.0.0.1
     * 2）从/etc/hosts找不到合适的IP地址，我们需要从网卡上面去获取NetworkInterface.getNetworkInterfaces()
     * 由于网卡存在不同的类型
     * docker    -- 一般以docker0 为网卡名字的
     * TUN/TAP   --tun0 ，tap等等代表构建VPN的时候创建的虚拟网卡
     * eth       --eth0， eth1为准的真实网卡名字，注意这个真实网卡名字不一定就是eth开头，所以通过名字来判断是不正确的
     * 我们需要过滤掉docker, TUN/TAP这些网卡，然后获取真实的网卡对应的IP地址
     * <p>
     * 其实用户自己指定是最好的，这样不会出现错误，程序自己获取的情况下，可能会存在一些获取不到真实IP的情况。这样发布出去，客服端连接不通
     * 也是问题
     *
     * @return 返回hostname:port的形式
     */
    String publish();

    /**
     * 服务器提供的的服务名字，可以任意指定一个服务名字，客户端会来根据该名字来获取metadata，或者发布到zookeeper上面去
     *
     * @return 当前服务提供的服务
     */
    String service();

    /**
     * 当前服务提供的权重，服务端会根据客服端提供的权重来创建连接，也就是权重越多，那么连接数也就是越多，那么访问量也就越多
     * 默认值创建一个Channel，测试发现，一个Channel开启合并flush操作的时候，性能是最好的，所以默认就是用这一个Channel就可以了
     * 如果某些服务提供者可以处理更多的请求，那么可以调整大一点儿。具体还是要看具体业务测试，依据业务测试为准，大多数一个就已经满足要求了
     *
     * @return 返回权重，也就是权重越高，那么访问量也就越大
     */
    Weight weight() default Weight.ONE;

    /**
     * 这个是服务器端的参数，主要是配置服务器工作组线程的数量，也就是有多少个线程来处理IO事件, 注意只是处理IO事件的线程数量
     * 如果是不同的service发布到相同的address上面去，谁先发布就用谁的works数量, 所以最好的做法是为每一个service单独分配一个端口
     * 让这个配置可以生效。不建议多个不同的RpcService，发布到同一个port上面去，相同的RpcService会merge。
     *
     * @return 返回工作组数量，也就是IO线程的数量
     */
    int works() default 8;
}
