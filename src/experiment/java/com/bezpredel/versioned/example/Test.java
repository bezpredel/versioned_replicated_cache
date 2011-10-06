package com.bezpredel.versioned.example;

import com.bezpredel.versioned.cache.CacheServiceInitializer;
import com.bezpredel.versioned.cache.replication.SlaveCacheServiceInitializer;
import com.bezpredel.versioned.example.data.Def;

import java.util.Arrays;

public class Test {
    public static void main(String [] args) {

        new GeneralAssembly().populateMaster();

    }
}
