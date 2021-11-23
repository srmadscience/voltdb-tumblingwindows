package arbitraryattributes;

import java.math.BigDecimal;

import org.voltdb.types.TimestampType;

public interface EventReporterInterface {

    public void reportEvent(String cardid, TimestampType txn_time, String txn_id, BigDecimal txn_amount,
            String txn_kind, String txn_tag1, long txn_tag1_value, String txn_tag2, long txn_tag2_value,
            String txn_tag3, long txn_tag3_value, String txn_tag4, long txn_tag4_value);

    public void deleteOldEvents(String cardid, String txn_kind, int max_age_minutes);

    public void raiseAlert(String cardid, String code, String message);
}
