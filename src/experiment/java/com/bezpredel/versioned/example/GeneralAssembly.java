package com.bezpredel.versioned.example;

import com.bezpredel.versioned.cache.AbstractImmutableCacheableObject;
import com.bezpredel.versioned.cache.SingleCacheService;
import com.bezpredel.versioned.cache.CacheServiceInitializer;
import com.bezpredel.versioned.cache.replication.SlaveCacheServiceInitializer;
import com.bezpredel.versioned.example.data.*;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.InputStream;
import java.util.*;

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

    private List<AbstractImmutableCacheableObject> parseInputFile(InputStream input) {
        SAXParserFactory factory = SAXParserFactory.newInstance();

        final List<AbstractImmutableCacheableObject> results = new ArrayList<AbstractImmutableCacheableObject>();
        try {
            SAXParser saxParser = factory.newSAXParser();
            saxParser.parse(
                input,
                new DefaultHandler() {
                    private final Object MARKER = new Object();
                    private ArrayDeque<Object> stack = new ArrayDeque<Object>();

                    int instrumentId = 0;
                    int marketId = 0;
                    int userId = 0;
                    int institutionId = 0;
                    Random rand = new Random();
                    @Override
                    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
                        if("market".equals(qName)) {
                            String name = attributes.getValue("name");
                            Market market = new Market(marketId++);
                            market.setName(name);
                            stack.push(market);
                            results.add(market);
                        } else if("bonds".equals(qName) || "stocks".equals(qName) ||  "etfs".equals(qName)) {
                            stack.push(qName);
                        } else if("instrument".equals(qName)) {
                            Instrument instrument = new Instrument(instrumentId++);
                            instrument.setActive(true);
                            results.add(instrument);
                            stack.push(instrument);
                        } else if("ticker".equals(qName) || "name".equals(qName)) {
                            stack.push(MARKER);
                        } else if("user".equals(qName)) {
                            User user = new User(userId++);
                            user.setFirstName(attributes.getValue("firstName"));
                            user.setLastName(attributes.getValue("lastName"));
                            user.setInstitutionId(rand.nextInt(institutionId));

                            results.add(user);
                        } else if("institution".equals(qName)) {
                            Institution institution = new Institution(institutionId++);
                            institution.setName(attributes.getValue("name"));

                            results.add(institution);
                        }
                    }

                    @Override
                    public void characters(char[] ch, int start, int length) throws SAXException {
                        if(stack.peek()!=MARKER) return;
                        String str = new String(ch, start, length).trim();
                        stack.push(str);
                    }

                    @Override
                    public void endElement(String uri, String localName, String qName) throws SAXException {
                        if("ticker".equals(qName) || "name".equals(qName)) {
                            String name = (String) stack.pop();
                            if(stack.pop()!=MARKER) {
                                throw new SAXException("Expected MARKER");
                            }
                            if("name".equals(qName)) {
                                ((Instrument)stack.peek()).setName(name);
                            } else {
                                ((Instrument)stack.peek()).setTicker(name);
                            }
                        } else if("instrument".equals(qName)) {
                            Instrument instrument = (Instrument) stack.pop();
                            String assetClassName = (String) stack.pop();
                            Market market = (Market) stack.peek();
                            stack.push(assetClassName);
                            instrument.setMarket(market);
                            if("bonds".equals(assetClassName)) {
                                instrument.setAssetType(AssetType.bond);
                            } else if("stocks".equals(assetClassName)) {
                                instrument.setAssetType(AssetType.stock);
                            } else if("etfs".equals(assetClassName)) {
                                instrument.setAssetType(AssetType.etf);
                            }
                        } else if("bonds".equals(qName) || "stocks".equals(qName) ||  "etfs".equals(qName)) {
                            stack.pop();
                        } if("market".equals(qName)) {
                            stack.pop();
                        }
                    }
                }
            );
        } catch (Exception e) {
            e.printStackTrace();
        }

        return results;
    }

    private void populateMaster(SingleCacheService.WriteContext context) {


        List<AbstractImmutableCacheableObject> marketsAndInstruments = parseInputFile(getClass().getResourceAsStream("testdata.xml"));
        for(AbstractImmutableCacheableObject obj : marketsAndInstruments) {
            context.put(obj);
        }


    }

    public void populateMaster() {
        masterAssembly.cacheService.executeWrite(new SingleCacheService.WriteCommand() {
            public void execute(SingleCacheService.WriteContext context) {
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
