package arbitraryattributes;

import java.math.BigDecimal;

import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

public interface ArbitraryEventProcessor {
    
    public int noteDetailsOfEvent(EventReporterInterface theReporter, String cardid, TimestampType txn_time, String txn_id, BigDecimal txn_amount,
            String txn_kind, String txn_tag1, long txn_tag1_value, String txn_tag2, long txn_tag2_value,
            String txn_tag3, long txn_tag3_value, String txn_tag4, long txn_tag4_value);

    public int raiseAlertsIfNeeded(EventReporterInterface theReporter, VoltTable totalsTable, VoltTable totalsByMinuteTable);

}
