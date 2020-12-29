package com.lee.rpc.helper.client;

import com.lee.rpc.MethodMetadata;
import com.lee.rpc.RpcException;
import com.lee.rpc.RpcMethodUnit;
import com.lee.rpc.annotation.RpcMethod;
import com.lee.rpc.annotation.Serialization;
import com.lee.rpc.decoder.RpcClientDecoder;
import com.lee.rpc.helper.RpcHelper;
import io.netty.channel.ChannelHandler;
import io.netty.util.collection.ByteObjectMap;
import javassist.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.builder.EqualsBuilder;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.lee.rpc.util.Constant.EMPTY;


@Slf4j
public class RpcClientGenerator {

    private final AtomicInteger rpcServiceCounter = new AtomicInteger(1);
    private final Map<String, RpcMethodUnit> setterMethodMapping = new HashMap<>();
    private final Map<Integer, Class<ChannelHandler>> registeredChannels = new HashMap<>();

    public synchronized ChannelHandler createDecoder(InetSocketAddress address,
                                                     int serviceId,
                                                     ClientProxy clientProxy) {
        try {
            RpcServiceClientUnit unit = RpcHelper.getRpcClientUnit(serviceId);

            if (registeredChannels.containsKey(serviceId)) {
                return registeredChannels.get(serviceId).
                        getConstructor(InetSocketAddress.class, ClientProxy.class, RpcServiceClientUnit.class)
                        .newInstance(address, clientProxy, unit);
            }

            ClassPool pool = ClassPool.getDefault();
            CtClass clientDecoder = pool.makeClass(RpcClientDecoder.class.getPackage().getName() + "." +
                    "RpcClientDecoder_" + serviceId
            );
            clientDecoder.setSuperclass(pool.get(RpcClientDecoder.class.getName()));

            CtField clientUintField = new CtField(
                    pool.get(RpcServiceClientUnit.class.getName()), "clientUnit", clientDecoder
            );
            clientUintField.setModifiers(Modifier.PRIVATE);
            clientDecoder.addField(clientUintField);

            CtConstructor cons = new CtConstructor(
                    new CtClass[]{pool.get(InetSocketAddress.class.getName()), pool.get(ClientProxy.class.getName()),
                            pool.get(RpcServiceClientUnit.class.getName())}, clientDecoder
            );
            cons.setBody("{$0.address=$1;$0.clientProxy=$2;$0.clientUnit = $3;}");
            cons.setModifiers(Modifier.PUBLIC);
            clientDecoder.addConstructor(cons);

            StringBuilder sb = new StringBuilder("switch ($2) { \n");
            //$0 代表this， $1,$2,$2...代表参数
            for (ByteObjectMap.PrimitiveEntry<RpcMethodUnit> methodUnit : unit.getMethodIdMapping().entries()) {
                //把所有的RpcMethodUnit作为字段放到类里面去
                String key = serviceId + "_" + methodUnit.key();
                CtField methodIdFiled = new CtField(
                        pool.get(RpcMethodUnit.class.getName()), "rpcMethod_" + key, clientDecoder
                );
                methodIdFiled.setModifiers(Modifier.PRIVATE);

                //添加字段
                clientDecoder.addField(methodIdFiled);

                //添加消息体
                sb.append("case ").append(methodUnit.key()).append(" : \n")
                        .append("if ( $0.rpcMethod_").append(key).append(" == null )").append(" { \n")
                        .append("$0.rpcMethod_").append(key).append(" = ")
                        .append("$0.clientUnit.getMethodUnit($2);").append("\n")
                        .append(" } \n")
                        .append(" return ").append("$0.rpcMethod_").append(key).append(";\n");
            }
            sb.append("default : \n").
                    append("throw new UnsupportedOperationException(\"Can not get RpcMethodUnit with id \" + $1);\n");
            sb.append("}").append("\n");

            //添加方法
            CtMethod ctMethod = new CtMethod(pool.get(RpcMethodUnit.class.getName()), "getRpcMethodUnit",
                    new CtClass[]{CtClass.intType, CtClass.byteType},
                    clientDecoder
            );

            ctMethod.setModifiers(Modifier.PROTECTED);
            ctMethod.setBody(sb.toString());
            clientDecoder.addMethod(ctMethod);

            clientDecoder.writeFile("/home/lee");

            @SuppressWarnings("unchecked")
            Class<ChannelHandler> newDecoder = (Class<ChannelHandler>) clientDecoder.toClass(
                    RpcClientDecoder.class.getClassLoader(),
                    RpcClientDecoder.class.getProtectionDomain()
            );

            registeredChannels.put(serviceId, newDecoder);
            clientDecoder.detach();
            return newDecoder.getConstructor(
                    InetSocketAddress.class, ClientProxy.class, RpcServiceClientUnit.class)
                    .newInstance(address, clientProxy, unit);
        } catch (Exception e) {
            throw new RpcException("Can not generate RpcMethodUnit", e);
        }
    }

    public synchronized ChannelHandler createChannelHandler(InetSocketAddress address, int serviceId) {
        try {
            if (registeredChannels.containsKey(serviceId)) {
                return registeredChannels.get(serviceId).getConstructor(InetSocketAddress.class).newInstance(address);
            }

            ClassPool pool = ClassPool.getDefault();
            CtClass clientDecoder = pool.makeClass(RpcClientDecoder.class.getPackage().getName() + "." +
                    "RpcClientDecoder_" + serviceId
            );
            clientDecoder.setSuperclass(pool.get(RpcClientDecoder.class.getName()));

            pool.importPackage("com.lee.netty.helper.RpcHelper");

            StringBuilder rpcClient = new StringBuilder("switch ($1) { \n");
            RpcServiceClientUnit unit = RpcHelper.getRpcClientUnit(serviceId);

            CtField rpcServiceClientUnitFiled = new CtField(
                    pool.get(RpcServiceClientUnit.class.getName()), "rpcClientUnit_" + serviceId, clientDecoder
            );
            rpcServiceClientUnitFiled.setModifiers(Modifier.PRIVATE);
            clientDecoder.addField(rpcServiceClientUnitFiled);

            CtMethod getRpcClientUnit = new CtMethod(
                    pool.get(RpcServiceClientUnit.class.getName()), "getRpcServiceClientUnit_" + serviceId,
                    new CtClass[]{CtClass.intType}, clientDecoder
            );
            getRpcClientUnit.setModifiers(Modifier.PRIVATE);
            getRpcClientUnit.setBody(
                    "{if ($0.rpcClientUnit_" + serviceId + " == null) { \n $0.rpcClientUnit_" + serviceId + "" +
                            " = RpcHelper.getRpcClientUnit($1);\n} " +
                            "return $0.rpcClientUnit_" + serviceId + ";}"
            );
            clientDecoder.addMethod(getRpcClientUnit);

            rpcClient.append("case ").append(serviceId).append(": \n");

            StringBuilder sb = new StringBuilder("switch ($2) { \n");
            //$0 代表this， $1,$2,$2...代表参数
            for (ByteObjectMap.PrimitiveEntry<RpcMethodUnit> methodUnit : unit.getMethodIdMapping().entries()) {
                //把所有的RpcMethodUnit 作为字段放到类里面去
                String key = serviceId + "_" + methodUnit.key();
                CtField methodIdFiled = new CtField(
                        pool.get(RpcMethodUnit.class.getName()), "rpcMethod_" + key, clientDecoder
                );
                methodIdFiled.setModifiers(Modifier.PRIVATE);

                //添加字段
                clientDecoder.addField(methodIdFiled);

                //添加消息体
                sb.append("case ").append(methodUnit.key()).append(" : \n")
                        .append("if ( $0.rpcMethod_").append(key).append(" == null )").append(" { \n")
                        .append("$0.rpcMethod_").append(key).append(" = ")
                        .append("$0.getRpcServiceClientUnit_").append(serviceId).append("($1).getMethodUnit($2);").append("\n")
                        .append(" } \n")
                        .append(" return ").append("$0.rpcMethod_").append(key).append(";\n");
            }
            sb.append("default : \n").
                    append("throw new UnsupportedOperationException(\"Can not get RpcMethodUnit with id \" + $1);\n");
            sb.append("}").append("\n");

            rpcClient.append(sb.toString());
            sb.setLength(0);

            rpcClient.append("default : \n").
                    append("throw new UnsupportedOperationException(\"Can not get RpcServiceClientUnit with serverId \" + $1);\n");
            rpcClient.append("}").append("\n");

            //添加方法
            CtMethod ctMethod = new CtMethod(pool.get(RpcMethodUnit.class.getName()), "getRpcMethodUnit",
                    new CtClass[]{CtClass.intType, CtClass.byteType},
                    clientDecoder
            );

            ctMethod.setModifiers(Modifier.PROTECTED);
            ctMethod.setBody(rpcClient.toString());
            clientDecoder.addMethod(ctMethod);

            clientDecoder.writeFile("/home/lee");

            @SuppressWarnings("unchecked")
            Class<ChannelHandler> newDecoder = (Class<ChannelHandler>) clientDecoder.toClass(
                    RpcClientDecoder.class.getClassLoader(),
                    RpcClientDecoder.class.getProtectionDomain()
            );
            registeredChannels.put(serviceId, newDecoder);
            clientDecoder.detach();
            return newDecoder.getConstructor(InetSocketAddress.class).newInstance(address);
        } catch (Exception e) {
            throw new RpcException("Can not generate RpcMethodUnit", e);
        }
    }

    public synchronized <T> T generate(Class<T> targetClass, int serviceId, ClientProxy clientProxy) {
        if (RpcHelper.isClientReady(serviceId)) {
            try {
                ClassPool pool = ClassPool.getDefault();
                CtClass rpcClient = pool.makeClass(NettyClient.class.getPackage().getName() + "." +
                        "RpcClient_" + serviceId + "_" + rpcServiceCounter.getAndIncrement()
                );
                rpcClient.setInterfaces(new CtClass[]{pool.get(targetClass.getName())});

                CtField ctField = new CtField(
                        pool.get(clientProxy.getClass().getName()), "nettyClient", rpcClient
                );
                ctField.setModifiers(Modifier.PRIVATE);
                rpcClient.addField(ctField);

                CtConstructor cons = new CtConstructor(
                        new CtClass[]{pool.get(clientProxy.getClass().getName())}, rpcClient
                );

                cons.setBody("{$0.nettyClient = $1;}");
                cons.setModifiers(Modifier.PUBLIC);
                rpcClient.addConstructor(cons);

                RpcServiceClientUnit rpcClientUnit = RpcHelper.getRpcClientUnit(serviceId);

                Method[] declaredMethods = targetClass.getDeclaredMethods();
                for (Method method : declaredMethods) {
                    Class<?>[] parameterTypes = method.getParameterTypes();

                    CtClass returnType;
                    if (!Void.TYPE.equals(method.getGenericReturnType())) {
                        returnType = pool.get(method.getReturnType().getName());
                    } else {
                        returnType = CtClass.voidType;
                    }
                    if (parameterTypes.length <= 1) {
                        //默认只要是在这个接口里面的所有方法，那么都作为RpcMethod方法来使用
                        RpcMethodUnit methodUnit = findMatchedRpcMethodUnit(method, rpcClientUnit);
                        if (methodUnit != null) {
                            generateRpcServiceClient(
                                    pool, returnType, method, methodUnit, rpcClient, serviceId
                            );
                        } else {
                            //没有找到符合metadata要求的方法，那么直接给该方法生成一个不支持的操作，也就是直接抛出一个错误
                            CtMethod notMatchedMethod = new CtMethod(returnType, method.getName(),
                                    parameterTypes.length <= 0 ? null :
                                            new CtClass[]{pool.get(parameterTypes[0].getName())}, rpcClient
                            );
                            notMatchedMethod.setModifiers(Modifier.PUBLIC);
                            notMatchedMethod.setBody(
                                    "{throw new UnsupportedOperationException(" +
                                            "\"Not support method with @RpcMethod," +
                                            " but metadata not find\");}"
                            );
                            rpcClient.addMethod(notMatchedMethod);
                        }
                    } else {
                        //暂时不支持多参数的模式，多参数个人感觉没有啥子必要，因为都可以封装成一个请求来发送的，没有必要发送多个参数
                        //所以这个地方暂时不考虑支持多参数的模式
                        CtClass[] parameters = new CtClass[parameterTypes.length];
                        for (int i = 0; i < parameterTypes.length; i++) {
                            parameters[i] = pool.get(parameterTypes[i].getName());
                        }
                        CtMethod notMatchedMethod = new CtMethod(returnType, method.getName(),
                                parameters,
                                rpcClient
                        );
                        notMatchedMethod.setModifiers(Modifier.PUBLIC);
                        notMatchedMethod.setBody(
                                "{throw new UnsupportedOperationException(\"Not support method with multiple parameters\");}"
                        );
                        rpcClient.addMethod(notMatchedMethod);
                    }
                }

                rpcClient.writeFile("/home/lee");

                Class<?> target = rpcClient.toClass();
                Object instance = target.getConstructor(clientProxy.getClass()).newInstance(clientProxy);
                for (Map.Entry<String, RpcMethodUnit> entry : setterMethodMapping.entrySet()) {
                    Method method = target.getMethod(entry.getKey(), RpcMethodUnit.class);
                    method.invoke(instance, entry.getValue());
                }

                rpcClient.detach();

                return targetClass.cast(instance);
            } catch (Exception e) {
                throw new RpcException("Can not generate proxy class for " + targetClass, e);
            } finally {
                setterMethodMapping.clear();
            }
        } else {
            try {
                TimeUnit.MILLISECONDS.sleep(1000);
                return generate(targetClass, serviceId, clientProxy);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RpcException("Can not be interrupted by any other thread", e);
            }
        }
    }

    private RpcMethodUnit findMatchedRpcMethodUnit(Method method, RpcServiceClientUnit rpcClientUnit) {
        for (ByteObjectMap.PrimitiveEntry<RpcMethodUnit> entry : rpcClientUnit.getMethodIdMapping().entries()) {
            RpcMethodUnit unit = entry.value();
            if (findMatchRpcMethod(unit, method)) {
                return unit;
            }
        }
        return null;
    }

    private void generateRpcServiceClient(ClassPool pool,
                                          CtClass returnType,
                                          Method method,
                                          RpcMethodUnit methodUnit, CtClass rpcClient, int serviceId) {
        try {
            String key = "field_" + serviceId + "_" + methodUnit.getMethodId();
            CtField methodUnitField = new CtField(
                    pool.get(methodUnit.getClass().getName()), key, rpcClient
            );
            methodUnitField.setModifiers(Modifier.PRIVATE);
            rpcClient.addField(methodUnitField);

            //添加setter方法，主要是为了把值设置进去
            String methodName = "setField_" + serviceId + "_" + methodUnit.getMethodId();
            rpcClient.addMethod(CtNewMethod.setter(methodName, methodUnitField));
            setterMethodMapping.put(methodName, methodUnit);

            //这个地方只能够有一个参数，所以直接获取
            Class<?>[] parameterTypes = method.getParameterTypes();
            CtMethod matchedMethod = new CtMethod(
                    returnType,
                    method.getName(),
                    parameterTypes.length <= 0 ? null : new CtClass[]{pool.get(parameterTypes[0].getName())},
                    rpcClient
            );
            matchedMethod.setModifiers(Modifier.PUBLIC);
            if (parameterTypes.length <= 0) {
                matchedMethod.setBody(
                        "{return ($r)$0.nettyClient.invoke($0." + key + ", null);}"
                );
            } else {
                matchedMethod.setBody(
                        "{return ($r)$0.nettyClient.invoke($0." + key + ", $1);}"
                );
            }
            rpcClient.addMethod(matchedMethod);
        } catch (Exception e) {
            throw new RpcException("Can not support implement this method " + method, e);
        }
    }

    /**
     * 如果客户端使用了@RpcMethod，但是服务器端并没有该方法的Metadata信息，就会导致找不到MethodUnit,所以就不匹配
     *
     * @param methodUnit 该方法的metadata信息
     * @param method     接口里面使用了@RpcMethod标注的方法
     */
    private boolean findMatchRpcMethod(RpcMethodUnit methodUnit, Method method) {
        MethodMetadata metadata = methodUnit.getMetadata();
        Class<?>[] parameterTypes = method.getParameterTypes();
        if (new EqualsBuilder()
                .append(method.getName(), metadata.getMethodName())
                .append(Void.TYPE.equals(method.getGenericReturnType()) ? EMPTY : method.getReturnType().getName(),
                        metadata.getReturnType())
                .append(parameterTypes.length <= 0 ? EMPTY : parameterTypes[0].getName(), metadata.getParameterType())
                .isEquals()) {
            //方法签名都相等的话，这个时候默认就找到了和metadata对应上的方法了，那么就可以给当前接口添加方法了
            Serialization serialization = method.getDeclaredAnnotation(Serialization.class);
            if (serialization != null) {
                try {
                    //默认已经设置了一个返回值的解析器，如果我们没有使用@Serialization注解来显示的指定我们自己的解析器的话，那么
                    //默认情况下就使用默认的，否则使用我们指定的
                    methodUnit.setReturnValueSerializer(serialization.value().getConstructor().newInstance());
                } catch (Exception e) {
                    log.warn("Can not use serialization " + serialization.value(), e);
                }
            }

            if (parameterTypes.length > 0) {
                Annotation[][] parameterAnnotations = method.getParameterAnnotations();
                //参数只有一个，所以这个直接获取就可以了
                Annotation[] annotations = parameterAnnotations[0];
                if (annotations != null && annotations.length > 0) {
                    for (Annotation annotation : annotations) {
                        if (annotation instanceof Serialization) {
                            Serialization parameterSer = (Serialization) annotation;
                            try {
                                methodUnit.setParameterSerializer(
                                        parameterSer.value().getConstructor().newInstance()
                                );
                            } catch (Exception e) {
                                log.warn("Can not use serialization " + parameterSer.value(), e);
                            }
                            break;
                        }
                    }
                }
                metadata.setParameterType(parameterTypes[0].getName());
            }
            return true;
        }
        return false;
    }
}
