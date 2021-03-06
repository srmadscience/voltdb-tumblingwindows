package arbitraryattributes;

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

import java.math.BigDecimal;

import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

public class HighValueTransactionObserver implements ArbitraryEventProcessor {

    private static final String HIGH_VALUE = "HIGH_VALUE";

    public HighValueTransactionObserver(long minTranSize, long alertThresold) {
        super();
        this.minTranSize = minTranSize;
        this.alertThresold = alertThresold;
    }

    long minTranSize;
    long alertThresold;

    @Override
    public int noteDetailsOfEvent(EventReporterInterface theReporter, String cardid, TimestampType txn_time,
            String txn_id, BigDecimal txn_amount, String txn_kind, String txn_tag1, long txn_tag1_value,
            String txn_tag2, long txn_tag2_value, String txn_tag3, long txn_tag3_value, String txn_tag4,
            long txn_tag4_value) {

        theReporter.deleteOldEvents(cardid,txn_kind,600);
        
        if (txn_amount.longValue() > minTranSize) {
            theReporter.reportEvent(cardid, txn_time, txn_id, txn_amount, HIGH_VALUE, txn_tag1, txn_tag1_value,
                    txn_tag2, txn_tag2_value, txn_tag3, txn_tag3_value, txn_tag4, txn_tag4_value);
            return 1;
        }
        
        return 0;

    }

    @Override
    public int raiseAlertsIfNeeded(EventReporterInterface theReporter, VoltTable totalsTable,
            VoltTable totalsByMinuteTable) {

        while (totalsTable.advanceRow()) {
            String txnKind = totalsTable.getString("txn_kind");

            if (txnKind.equalsIgnoreCase(HIGH_VALUE)) {
                BigDecimal howMuch = totalsTable.getDecimalAsBigDecimal("how_much");

                if (howMuch.longValue() > alertThresold) {
                    theReporter.raiseAlert(totalsTable.getString("cardid"), "OVERSPEND",
                            "Too much spent on high value transactions:" + howMuch);
                    return 1;
                }

                break;
            }
        }
        
        return 0;

    }

}
