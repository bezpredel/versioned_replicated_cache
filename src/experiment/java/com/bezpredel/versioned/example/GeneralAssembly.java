package com.bezpredel.versioned.example;

import com.bezpredel.versioned.cache.CacheService;
import com.bezpredel.versioned.cache.CacheServiceInitializer;
import com.bezpredel.versioned.cache.replication.SlaveCacheServiceInitializer;
import com.bezpredel.versioned.example.data.Def;
import com.bezpredel.versioned.example.data.Market;

import java.util.Arrays;

/**
 * Date: 10/4/11
 * Time: 3:22 PM
 */
public class GeneralAssembly {
    public final MasterAssembly masterAssembly;
    public final SlaveAssembly slave1;
    public final SlaveAssembly slave2;

    public GeneralAssembly() {

        masterAssembly = new MasterAssembly(populateInitialized(new CacheServiceInitializer(), "Market Cache Service"));

        slave1 = new SlaveAssembly(populateInitialized(new SlaveCacheServiceInitializer(), "Slave MC 1"), masterAssembly);
        slave2 = new SlaveAssembly(populateInitialized(new SlaveCacheServiceInitializer(), "Slave MC 2"), masterAssembly);

    }

    private void populateMaster(CacheService.WriteContext context) {
        Market market1 = new Market("NYSE");
        Market market2 = new Market("NASDAQ");
        Market market3 = new Market("AMEX");

        context.put(market1);
        context.put(market2);
        context.put(market3);





        //todo: implement
    }

    public void populateMaster() {
        masterAssembly.cacheService.executeWrite(new CacheService.WriteCommand() {
            public void execute(CacheService.WriteContext context) {
                populateMaster(context);
            }
        });
    }

    public static <T extends CacheServiceInitializer> T populateInitialized(T initializer, String name) {
        initializer.setCacheSpecs(
            Arrays.asList(Def.INSTITUTION_CACHE, Def.INSTRUMENT_CACHE, Def.MARKET_CACHE, Def.ORDER_CACHE, Def.USER_CACHE)
        );

        initializer.setName(name);
        initializer.setUnlockAsynchronously(false);
        initializer.setWriteLock(new Object());

        return initializer;
    }


}
