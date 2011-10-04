package com.bezpredel.collections;


import com.google.common.base.Function;

import java.util.HashSet;
import java.util.Set;

public class Utils {
    public static <I, O> Set<O> map(Set<I> set, Function<I, O> transform) {
        HashSet<O> out = new HashSet<O>(set.size());
        for(I in : set) {
            out.add(transform.apply(in));
        }

        return out;
    }
}
