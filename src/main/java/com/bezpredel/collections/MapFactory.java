package com.bezpredel.collections;

import java.util.Map;

public interface MapFactory {
    <K, V> Map<K, V> createMap(Class<K> keyClass);
}
