package com.bezpredel.collections;

import com.google.common.base.Function;
import com.google.common.base.Throwables;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class FieldGetterFunction<O, R> implements Function<O, R> {
    private final Field field;

    public FieldGetterFunction(Class<? extends O> clazz, String name) throws NoSuchFieldException {
        this(clazz, name, true);
    }

    public FieldGetterFunction(Class<? extends O> clazz, String name, boolean nonPublic) throws NoSuchFieldException {
        if(nonPublic) {
            field = clazz.getDeclaredField(name);
            field.setAccessible(true);
        } else {
            field = clazz.getField(name);
        }
    }

    public R apply(@Nullable O input) {
        if(input==null) {
            return null;
        } else {
            try {
                return (R)field.get(input);
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
