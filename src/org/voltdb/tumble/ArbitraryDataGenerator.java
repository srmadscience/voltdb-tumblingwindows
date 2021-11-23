package org.voltdb.tumble;

/* This file is part of VoltDB.
 * Copyright (C) 2008-2021 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

import java.io.IOException;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.types.TimestampType;

public class ArbitraryDataGenerator {

    Client voltClient = null;

    String hostnames;
    int userCount;
    int tpMs;
    int durationSeconds;

    // Note that we generate the same 'random' values each time we run
    Random r = new Random(42);

    public ArbitraryDataGenerator(String hostnames, int userCount, int tpMs, int durationSeconds)
            throws Exception {

        this.hostnames = hostnames;
        this.userCount = userCount;
        this.tpMs = tpMs;
        this.durationSeconds = durationSeconds;
 
        msg("hostnames=" + hostnames + ", users=" + userCount + ", tpMs=" + tpMs + ",durationSeconds=" + durationSeconds
                );

        msg("Log into VoltDB");
        voltClient = connectVoltDB(hostnames);

    }

    public void run(int offset) {

        long startMs = System.currentTimeMillis();
        long currentMs = System.currentTimeMillis();

        int tpThisMs = 0;
        long recordCount = 0;

        ComplainOnErrorCallback coec = new ComplainOnErrorCallback();

        while (System.currentTimeMillis() < (startMs + (1000 * durationSeconds))) {

            recordCount++;

            String randomCardNumber = "Card" + (r.nextInt(userCount) + offset);

            try {

                String cardid = randomCardNumber;
                TimestampType txn_time = new TimestampType();
                String txn_id = "Tran" + recordCount;
                BigDecimal txn_amount = new BigDecimal(r.nextInt(1000));
                
                String txn_kind = "InStorePurchase";
                String txn_tag1 = "Groceries";
                
                if (r.nextInt(100) == 0) {
                    txn_tag1 = "Cellphone";
                }
                
                long txn_tag1_value = 1;
                String txn_tag2 = "Postcode";
                long txn_tag2_value = r.nextInt(100);
                String txn_tag3 = "Temperture";
                long txn_tag3_value =  5 + r.nextInt(15);;
                String txn_tag4 = null;
                long txn_tag4_value = 0;
               voltClient.callProcedure(coec, "ReportArbitraryEvent", cardid, txn_time, txn_id, txn_amount,
                            txn_kind, txn_tag1, txn_tag1_value, txn_tag2, txn_tag2_value, txn_tag3, txn_tag3_value,
                            txn_tag4, txn_tag4_value);
              
            } catch (IOException e1) {
                msg(e1.getMessage());
            }

            if (tpThisMs++ > tpMs) {

                // but sleep if we're moving too fast...
                while (currentMs == System.currentTimeMillis()) {
                    try {
                        Thread.sleep(0, 50000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                currentMs = System.currentTimeMillis();
                tpThisMs = 0;
            }

            if (recordCount % 100000 == 0) {
                msg("RecordCount=" + recordCount);
                printApplicationStats();
            }

        }

        
        try {
            voltClient.drain();
            voltClient.close();
        } catch (Exception e) {
            msg(e.getMessage());
        }

    }

    /**
     * Check VoltDB to see how things are going...
     *
     * @param client
     * @param nextCdr
     */
    public void printApplicationStats() {

        final String testCardId = "Card42";

        try {
            voltClient.drain();

            ClientResponse cr[] = null;

           
                cr = new ClientResponse[3];
                cr[0] = voltClient.callProcedure("@AdHoc", " select * from cc_agg_attribute_table_totals where cardid = '"
                        + testCardId + "' order by TXN_TIME;");
                cr[1] = voltClient.callProcedure("@AdHoc", " SELECT txn_kind ,txn_tag1 ,txn_tag2 ,txn_tag3 ,txn_tag4 , count(*) how_many , sum(txn_tag1_value) txn_tag1_value , sum(txn_tag2_value) txn_tag2_value , sum(txn_tag3_value) txn_tag3_value , sum(txn_tag4_value) txn_tag4_value , sum(how_much) how_much from cc_agg_attribute_table_totals WHERE cardid = '"
                        + testCardId + "' GROUP BY txn_kind, txn_tag1, txn_tag2, txn_tag3, txn_tag4 ORDER BY txn_kind, txn_tag1, txn_tag2, txn_tag3, txn_tag4;");
                cr[2] = voltClient.callProcedure("@AdHoc", " select * from cc_agg_alert where cardid = '"
                        + testCardId + "' order by TXN_TIME;");
           

            if (cr != null) {
                msg("");
                msg("Results for " + testCardId);
                for (ClientResponse element : cr) {
                    if (element.getStatus() == ClientResponse.SUCCESS) {
                        for (int j = 0; j < element.getResults().length; j++) {
                            msg("\n" + element.getResults()[j].toFormattedString());
                        }
                    }
                }
            }

        } catch (Exception e) {
            msg("Error:" + e.getMessage());
        }

    }

    public static void main(String[] args) throws Exception {

        if (args.length != 5) {
            msg("Usage: ArbitraryDataGenerator hostnames userCount tpMs durationSeconds offset ");
            System.exit(1);
        }

        String hostnames = args[0];
        int userCount = Integer.parseInt(args[1]);
        int tpMs = Integer.parseInt(args[2]);
        int durationSeconds = Integer.parseInt(args[3]);
        int offset = Integer.parseInt(args[4]);
       

        ArbitraryDataGenerator a = new ArbitraryDataGenerator(hostnames, userCount, tpMs, durationSeconds);

        a.run(offset);

    }

    /**
     *
     * Connect to VoltDB using native APIS
     *
     * @param commaDelimitedHostnames
     * @return
     * @throws Exception
     */
    private static Client connectVoltDB(String commaDelimitedHostnames) throws Exception {
        Client client = null;
        ClientConfig config = null;

        try {
            msg("Logging into VoltDB");

            config = new ClientConfig(); // "admin", "idontknow");
            config.setTopologyChangeAware(true);
            config.setReconnectOnConnectionLoss(true);

            client = ClientFactory.createClient(config);

            String[] hostnameArray = commaDelimitedHostnames.split(",");

            for (String element : hostnameArray) {
                msg("Connect to " + element + "...");
                try {
                    client.createConnection(element);
                } catch (Exception e) {
                    msg(e.getMessage());
                }
            }

            msg("Connected to VoltDB");

        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("VoltDB connection failed.." + e.getMessage(), e);
        }

        return client;

    }

    /**
     * Print a formatted message.
     *
     * @param message
     */
    public static void msg(String message) {

        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date now = new Date();
        String strDate = sdfDate.format(now);
        System.out.println(strDate + ":" + message);

    }

}
