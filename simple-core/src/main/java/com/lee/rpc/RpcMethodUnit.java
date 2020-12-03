package com.lee.rpc;

import com.lee.rpc.annotation.Serialization;
import com.lee.rpc.executor.AbstractExecutor;
import com.lee.rpc.schema.Serializer;
import com.lee.rpc.schema.primitives.*;
import com.lee.rpc.util.stream.ByteBufInputStream;
import com.lee.rpc.util.stream.ByteBufOutputStream;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ClassUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import static com.lee.rpc.util.Constant.EMPTY;

/**
 * @author Administrator
 */
@Data
@Slf4j
public class RpcMethodUnit {

    @SuppressWarnings("rawtypes")
    private Serializer returnValueSerializer;

    @SuppressWarnings("rawtypes")
    private Serializer parameterSerializer;

    private byte methodId;
    private Method serviceMethod;
    private Object serviceInstance;
    private MethodMetadata metadata;

    private String group;
    private Executor executor;

    /**
     * Method方法的参数的数量必须要相同，就算不想设置值，那么该参数也必须使用null来表示
     *
     * @param parameter 需要的参数，参数个数必须和Method声明里面的个数相等
     * @return 返回方法调用的值，如果Method返回值声明为void，那么返回null, 有返回值，如果返回值本身为null，那么值也为null
     */
    public Object invoke(Object parameter) {
        try {
            return serviceMethod.invoke(serviceInstance, parameter);
        } catch (Exception e) {
            return new RpcException("Can not invoke serviceMethod " + serviceMethod.getName(), e);
        }
    }

    @SuppressWarnings("unchecked")
    public void serializeParameter(Object body, ByteBufOutputStream out) {
        parameterSerializer.serialize(body, out);
    }

    @SuppressWarnings("unchecked")
    public void serializeReturnValue(Object body, ByteBufOutputStream out) {
        returnValueSerializer.serialize(body, out);
    }

    public Object deserializeToParameter(ByteBufInputStream in) {
        return parameterSerializer.deserialize(in);
    }

    public Object deserializeToReturnType(ByteBufInputStream in) {
        return returnValueSerializer.deserialize(in);
    }

    public RpcMethodUnit withMethod(Method method, Object instance) {
        prepareMethod(method);
        this.serviceInstance = instance;
        return this;
    }

    private void prepareMethod(Method serviceMethod) {
        this.serviceMethod = serviceMethod;
        this.metadata = new MethodMetadata();

        metadata.setMethodName(serviceMethod.getName());

        if (!Void.TYPE.equals(serviceMethod.getGenericReturnType())) {
            Serialization serialization = serviceMethod.getDeclaredAnnotation(Serialization.class);
            if (serialization != null) {
                @SuppressWarnings("rawtypes")
                Class<? extends Serializer> serialize = serialization.value();
                try {
                    returnValueSerializer = serialize.getConstructor().newInstance();
                } catch (Exception e) {
                    log.warn("Can not use serializer " + serialize + " user default serializer", e);
                    returnValueSerializer = new ProtoStuffSerializer<>(serviceMethod.getReturnType());
                }
            } else {
                Class<?> returnType = serviceMethod.getReturnType();
                if (ClassUtils.isPrimitiveOrWrapper(returnType)) {
                    returnValueSerializer = getPrimitiveSerializer(returnType);
                } else {
                    returnValueSerializer = new ProtoStuffSerializer<>(returnType);
                }
            }
            metadata.setReturnType(serviceMethod.getReturnType().getName());
        } else {
            //如果参数为null的话，那么直接设置成EMPTY
            metadata.setReturnType(EMPTY);
        }

        //现目前支持一个参数，后面如果需要支持多参数，那么在修改成多参数模式，之前其实是可以支持多参数的，但是为了
        //简单和通用性起见，就修改成了单参数，多参数实际上是么有必要的
        Class<?>[] parameters = serviceMethod.getParameterTypes();
        if (parameters.length > 0) {
            Annotation[][] parameterAnnotations = serviceMethod.getParameterAnnotations();
            if (parameterAnnotations.length > 0) {
                Annotation[] annotations = parameterAnnotations[0];
                if (annotations != null) {
                    boolean isFound = false;
                    for (Annotation annotation : annotations) {
                        if (annotation instanceof Serialization) {
                            Serialization parameterSer = (Serialization) annotation;
                            @SuppressWarnings("rawtypes")
                            Class<? extends Serializer> deserialize = parameterSer.value();
                            try {
                                parameterSerializer = deserialize.getConstructor().newInstance();
                            } catch (Exception e) {
                                log.info("Can not use deserializer " + deserialize, e);
                                parameterSerializer = new ProtoStuffSerializer<>(parameters[0]);
                            }
                            isFound = true;
                        }
                    }

                    if (!isFound) {
                        if (ClassUtils.isPrimitiveOrWrapper(parameters[0])) {
                            parameterSerializer = getPrimitiveSerializer(parameters[0]);
                        } else {
                            parameterSerializer = new ProtoStuffSerializer<>(parameters[0]);
                        }
                    }
                } else {
                    if (ClassUtils.isPrimitiveOrWrapper(parameters[0])) {
                        parameterSerializer = getPrimitiveSerializer(parameters[0]);
                    } else {
                        parameterSerializer = new ProtoStuffSerializer<>(parameters[0]);
                    }
                }
                metadata.setParameterType(parameters[0].getName());
            } else {
                metadata.setParameterType(EMPTY);
            }
        } else {
            metadata.setParameterType(EMPTY);
        }
    }

    public RpcMethodUnit withReturnType(String returnType) {
        if (!returnType.equals(EMPTY)) {
            try {
                Class<?> returnTypeClass = Class.forName(returnType);
                if (ClassUtils.isPrimitiveOrWrapper(returnTypeClass)) {
                    returnValueSerializer = getPrimitiveSerializer(returnTypeClass);
                } else {
                    returnValueSerializer = new ProtoStuffSerializer<>(returnTypeClass);
                }
            } catch (ClassNotFoundException e) {
                log.error("Can not find Class " + returnType, e);
            }
        }
        return this;
    }

    public RpcMethodUnit withParameterType(String parameterType) {
        if (!parameterType.equals(EMPTY)) {
            try {
                Class<?> parameterTypeClass = Class.forName(parameterType);
                if (ClassUtils.isPrimitiveOrWrapper(parameterTypeClass)) {
                    parameterSerializer = getPrimitiveSerializer(parameterTypeClass);
                } else {
                    parameterSerializer = new ProtoStuffSerializer<>(parameterTypeClass);
                }
            } catch (ClassNotFoundException e) {
                log.error("Can not find Class " + parameterType, e);
            }
        }
        return this;
    }

    private Serializer<?> getPrimitiveSerializer(Class<?> keyType) {
        Class<?> primitive = ClassUtils.wrapperToPrimitive(keyType);
        switch (primitive.toGenericString()) {
            case "boolean":
                return BooleanSerializer.BOOLEAN_SERIALIZER;
            case "byte":
                return ByteSerializer.BYTE_SERIALIZER;
            case "char":
                return CharSerializer.CHAR_SERIALIZER;
            case "short":
                return ShortSerializer.SHORT_SERIALIZER;
            case "int":
                return IntSerializer.INT_SERIALIZER;
            case "long":
                return LongSerializer.LONG_SERIALIZER;
            case "double":
                return DoubleSerializer.DOUBLE_SERIALIZER;
            case "float":
                return FloatSerializer.FLOAT_SERIALIZER;
            default:
                throw new IllegalArgumentException("Can not recognize " + keyType);
        }
    }

    public MethodMetadata getMetadata() {
        return metadata;
    }

    public void setMethodId(byte methodId) {
        if (metadata != null) {
            metadata.setMethodId(methodId);
        }
    }

    public RpcMethodUnit withMetadata(MethodMetadata metadata) {
        this.metadata = metadata;
        return this;
    }

    public Executor getExecutor() {
        return executor;
    }

    public RpcMethodUnit withExecutor(Executor executor) {
        this.executor = executor;
        return this;
    }

    public RpcMethodUnit withGroup(String group) {
        this.group = group;
        return this;
    }

    public RpcMethodUnit withMethodId(byte methodId) {
        this.methodId = methodId;
        if (this.metadata != null) {
            this.metadata.setMethodId(methodId);
        }
        return this;
    }

    public void shutdown() {
        if (executor instanceof AbstractExecutor) {
            ((AbstractExecutor) executor).stop();
            return;
        }

        if (executor instanceof ExecutorService) {
            ((ExecutorService) executor).shutdown();
        }
    }

    public ExecutorService getInternalExecutor() {
        if (executor instanceof AbstractExecutor) {
            Executor internalExecutor = ((AbstractExecutor) executor).getInternalExecutor();
            if (internalExecutor instanceof ExecutorService) {
                return (ExecutorService) internalExecutor;
            }
            return null;
        }

        if (executor instanceof ExecutorService) {
            return (ExecutorService) executor;
        }
        return null;
    }
}
