package com.bezpredel.versioned.example.data;

import com.bezpredel.collections.GetterMethodFunction;
import com.bezpredel.versioned.cache.ImmutableCacheableObject;
import com.bezpredel.versioned.cache.def.CacheSpec;
import com.bezpredel.versioned.cache.def.IndexSpec;
import com.google.common.base.Function;

import java.util.Arrays;
import java.util.Collections;

public interface Def {
    CacheSpec ORDER_CACHE = new CacheSpec(
        Order.CACHE,
        Arrays.asList(
            new IndexSpec(
                Order.BIDS,
                new Function<ImmutableCacheableObject, Object>() {
                    public Object apply(ImmutableCacheableObject input) {
                        Order order = (Order) input;
                        if(order.getSide()==Side.bid) {
                            return order.getInstrumentId();
                        } else {
                            return null;
                        }
                    }
                },
                false, true
            ),
            new IndexSpec(
                Order.OFFERS,
                new Function<ImmutableCacheableObject, Object>() {
                    public Object apply(ImmutableCacheableObject input) {
                        Order order = (Order) input;
                        if(order.getSide()==Side.offer) {
                            return order.getInstrumentId();
                        } else {
                            return null;
                        }
                    }
                },
                false, true
            ),
            new IndexSpec(
                Order.BY_USER,
                new GetterMethodFunction<ImmutableCacheableObject, Object>(Order.class, "getUserId"),
                false, true
            )
        )
    );

    CacheSpec USER_CACHE = new CacheSpec (
        User.CACHE,
        Arrays.asList(
            new IndexSpec(
                User.BY_INSTITUTION,
                new GetterMethodFunction<ImmutableCacheableObject, Object>(User.class, "getInstitutionId"),
                false, true
            )
        )
    );

    CacheSpec INSTITUTION_CACHE = new CacheSpec(
        Institution.CACHE,
        Collections.<IndexSpec>emptySet()
    );

    CacheSpec MARKET_CACHE = new CacheSpec(
        Market.CACHE,
        Collections.<IndexSpec>emptySet()
    );

    CacheSpec INSTRUMENT_CACHE = new CacheSpec (
        Instrument.CACHE,
        Arrays.asList(
            new IndexSpec(
                Instrument.BY_MARKET,
                new GetterMethodFunction<ImmutableCacheableObject, Object>(Instrument.class, "getMarketId"),
                false, true
            ),
            new IndexSpec(
                Instrument.BY_TICKER,
                new GetterMethodFunction<ImmutableCacheableObject, Object>(Instrument.class, "getTicker"),
                false, false
            )
        )
    );

}
