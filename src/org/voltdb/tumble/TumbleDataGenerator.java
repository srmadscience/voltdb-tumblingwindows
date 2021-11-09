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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;

public class TumbleDataGenerator {

    public final static String ARBITRARYTUMBLE = "ARBITRARYTUMBLE";
    public final static String TUMBLE = "TUMBLE";
    public final static String HOP = "HOP";
    public final static String SLIDE = "SLIDE";
    public final static String SESSION = "SESSION";

    Client voltClient = null;

    String hostnames;
    int userCount;
    int tpMs;
    int durationSeconds;
    long startMs;
    String goal;

    Random r = new Random(42);

    public TumbleDataGenerator(String hostnames, int userCount, int tpMs, int durationSeconds, String goal)
            throws Exception {

        this.hostnames = hostnames;
        this.userCount = userCount;
        this.tpMs = tpMs;
        this.durationSeconds = durationSeconds;
        this.goal = goal;

        msg("hostnames=" + hostnames + ", users=" + userCount + ", tpMs=" + tpMs + ",durationSeconds=" + durationSeconds
                + ", goal=" + goal);

        msg("Log into VoltDB");
        voltClient = connectVoltDB(hostnames);

    }

    public void run(int offset) {

        startMs = System.currentTimeMillis();

        long currentMs = System.currentTimeMillis();
        int tpThisMs = 0;
        long recordCount = 0;

        ComplainOnErrorCallback coec = new ComplainOnErrorCallback();

        while (System.currentTimeMillis() < (startMs + (1000 * durationSeconds))) {

            recordCount++;

            String randomCardNumber = "Card" + (r.nextInt(userCount) + offset);

            try {

                long amount = r.nextInt(1000);
                long store = r.nextInt(100);
                
                if (goal.equalsIgnoreCase(HOP) || goal.equalsIgnoreCase(TUMBLE)) {
                    voltClient.callProcedure(coec, "cc_event_stream.INSERT", randomCardNumber, new Date(), recordCount,
                            amount, store);
                } else if (goal.equalsIgnoreCase(ARBITRARYTUMBLE)) {
                    voltClient.callProcedure(coec, "ReportArbitraryTumblingWindowEvent", randomCardNumber, recordCount, amount,
                            store, 300 * 1000, new Date(27000));
                } else if (goal.equalsIgnoreCase(SLIDE)) {
                    voltClient.callProcedure(coec, "ReportSlidingWindowEvent", randomCardNumber, recordCount, amount,
                            store);
                } else if (goal.equalsIgnoreCase(SESSION)) {
                    voltClient.callProcedure(coec, "ReportSessionWindowEvent", randomCardNumber, recordCount, amount,
                            store, 119, 19000);
                } else {
                    msg("Unknown goal of " + goal);
                    System.exit(1);
                }
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

        if (goal.equalsIgnoreCase(SESSION)) {
            msg("Waiting around two minutes for stale sessions to be spotted");

            try {
                Thread.sleep(130 * 1000);
                printApplicationStats();
            } catch (InterruptedException e1) {
                msg(e1.getMessage());
            }
        }

        try {
            voltClient.drain();
            voltClient.close();
        } catch (Exception e) {
            msg(e.getMessage());
        }

    }

    public static void main(String[] args) throws Exception {

        if (args.length != 6) {
            msg("Usage: TumbleDataGenerator hostnames userCount tpMs durationSeconds offset goal");
            msg("where 'goal' is one of " + TUMBLE + " " + ARBITRARYTUMBLE + " " + HOP + " " + SLIDE + " or " + SESSION);
            System.exit(1);
        }

        String hostnames = args[0];
        int userCount = Integer.parseInt(args[1]);
        int tpMs = Integer.parseInt(args[2]);
        int durationSeconds = Integer.parseInt(args[3]);
        int offset = Integer.parseInt(args[4]);
        String goal = args[5];

        TumbleDataGenerator a = new TumbleDataGenerator(hostnames, userCount, tpMs, durationSeconds, goal);

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

            if (goal.equalsIgnoreCase(HOP)) {
                cr = new ClientResponse[2];
                cr[0] = voltClient.callProcedure("@AdHoc", " select * from CC_EVENT_BY_CARD_BY_MINUTE where cardid = '"
                        + testCardId + "' order by TXN_TIME;");
                cr[1] = voltClient.callProcedure("@AdHoc", " select * from CC_EVENT_HOPPING_WINDOW where cardid = '"
                        + testCardId + "' order by REPORT_TIME;");
            } else if (goal.equalsIgnoreCase(TUMBLE)) {
                cr = new ClientResponse[2];
                cr[0] = voltClient.callProcedure("@AdHoc", " select * from CC_EVENT_BY_CARD_BY_MINUTE where cardid = '"
                        + testCardId + "' order by TXN_TIME;");
                cr[1] = voltClient.callProcedure("@AdHoc", " select * from CC_EVENT_TUMBLING_WINDOW where cardid = '"
                        + testCardId + "' order by REPORT_TIME;");
            } else if (goal.equalsIgnoreCase(ARBITRARYTUMBLE)) {
                cr = new ClientResponse[1];
                cr[0] = voltClient.callProcedure("@AdHoc", " select * from cc_event_arbitrary_tumbling_window where cardid = '"
                        + testCardId + "' order by REPORT_TIME;");
           } else if (goal.equalsIgnoreCase(SLIDE)) {
                cr = new ClientResponse[1];
                cr[0] = voltClient.callProcedure("@AdHoc", " select * from CC_EVENT_SLIDING_WINDOW where cardid = '"
                        + testCardId + "' order by REPORT_TIME;");
            } else if (goal.equalsIgnoreCase(SESSION)) {
                cr = new ClientResponse[2];
                cr[0] = voltClient.callProcedure("@AdHoc",
                        " select * from CC_EVENT_LAST_20 where cardid = '" + testCardId + "';");
                cr[1] = voltClient.callProcedure("@AdHoc", " select * from CC_EVENT_SESSION_WINDOW where cardid = '"
                        + testCardId + "' order by REPORT_TIME;");
            } else {
                msg("Unknown goal of " + goal);
                System.exit(1);
            }

            if (cr != null) {
                msg("");
                msg("Results for " + testCardId);
                for (int i = 0; i < cr.length; i++) {
                    if (cr[i].getStatus() == ClientResponse.SUCCESS) {
                        for (int j = 0; j < cr[i].getResults().length; j++) {
                            msg("\n" + cr[i].getResults()[j].toFormattedString());
                        }
                    }
                }
            }

        } catch (Exception e) {
            msg("Error:" + e.getMessage());
        }



    }

}
