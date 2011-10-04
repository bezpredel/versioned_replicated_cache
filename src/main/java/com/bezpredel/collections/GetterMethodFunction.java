package com.bezpredel.collections;

import com.google.common.base.Function;
import com.google.common.base.Throwables;

import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class GetterMethodFunction<O, R> implements Function<O, R> {
    private static final Object[] empty_obj_array = new Object[0];
    private final Method method;

    public GetterMethodFunction(Class<? extends O> clazz, String name) {
        this(clazz, name, false);
    }

    public GetterMethodFunction(Class<? extends O> clazz, String name, boolean nonPublic) {
        try {
            if(nonPublic) {
                method = clazz.getDeclaredMethod(name);
                method.setAccessible(true);
            } else {
                method = clazz.getMethod(name);
            }
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public R apply(@Nullable O input) {
        if(input==null) {
            return null;
        } else {
            try {
                return (R)method.invoke(input, empty_obj_array);
            } catch (Throwable e) {
                if(e instanceof InvocationTargetException) {
                    e = e.getCause();
                }
                Throwables.propagate(e);
                return null;//never reached, make the compiler happy
            }
        }
    }
}
