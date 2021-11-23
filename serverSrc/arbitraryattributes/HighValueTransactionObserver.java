package arbitraryattributes;

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
