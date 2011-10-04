package com.bezpredel.collections;

import java.util.HashMap;
import java.util.Map;

public abstract class PseudoEnum {
    private transient final int ordinal;

    protected PseudoEnum() {
        ordinal = getOrdinal(this.getClass());
    }

    protected int getOrdinal() {
        return ordinal;
    }

    private static final Map<Class<? extends PseudoEnum>, Integer> counters = new HashMap<Class<? extends PseudoEnum>, Integer>();

    private static int getOrdinal(Class<? extends PseudoEnum> clazz) {
        synchronized (counters) {
            Integer i = counters.get(clazz);
            if(i==null) {
                counters.put(clazz, Integer.valueOf(1));
                return 0;
            } else {
                counters.put(clazz, Integer.valueOf(i.intValue() + 1));
                return i;
            }
        }
    }

    public static int getCurrentCapacity(Class<? extends PseudoEnum> clazz) {
        try {
            Integer i = counters.get(clazz);
            return i==null ? 0 : i.intValue();
        } catch (Exception e) {
            return 0;
        }
    }

    @Override
    public final boolean equals(Object obj) {
        return (this == obj);
    }

    @Override
    public final int hashCode() {
        return ordinal;
    }
}
