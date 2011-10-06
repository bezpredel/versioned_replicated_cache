package com.bezpredel.versioned.example;

public class Test {
    public static void main(String [] args) throws Exception {
        org.apache.log4j.BasicConfigurator.configure();


        GeneralAssembly ga = new GeneralAssembly();
        ga.slave1.syncClient.start();
        ga.populateMaster();
        ga.slave2.syncClient.start();

        Thread.sleep(1000);

        ga.slave1.syncClient.stop();
        ga.slave2.syncClient.stop();
    }
}
