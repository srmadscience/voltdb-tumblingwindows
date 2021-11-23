package arbitraryattributes;

import java.math.BigDecimal;

import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

public class PhonePurchaseTransactionObserver implements ArbitraryEventProcessor {

  

    private int maxNumberOfPhonePurchases;
    
 
    public PhonePurchaseTransactionObserver(int maxNumberOfPhonePurchases) {
        super();
        this.maxNumberOfPhonePurchases = maxNumberOfPhonePurchases;
    }

    private static final String BUYPHONE = "BUYPHONE";
    private static final String CELLPHONE = "Cellphone";
  
    @Override
    public int noteDetailsOfEvent(EventReporterInterface theReporter, String cardid, TimestampType txn_time,
            String txn_id, BigDecimal txn_amount, String txn_kind, String txn_tag1, long txn_tag1_value,
            String txn_tag2, long txn_tag2_value, String txn_tag3, long txn_tag3_value, String txn_tag4,
            long txn_tag4_value) {
        
        theReporter.deleteOldEvents(cardid,txn_kind,600);
   
        // txn_tag1 is used to say what kind of thing was bought
        if (txn_tag1.equalsIgnoreCase(CELLPHONE)) {
            theReporter.reportEvent(cardid, txn_time, txn_id, txn_amount, BUYPHONE, txn_tag1, txn_tag1_value,
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

            if (txnKind.equalsIgnoreCase(CELLPHONE)) {
                long howMany = totalsTable.getLong("how_many");

                if (howMany > maxNumberOfPhonePurchases) {
                    theReporter.raiseAlert(totalsTable.getString("cardid"),
                            "MANYPHONES",
                            "Too many phone purchases:" + howMany);
                    return 1;
                }
                

                break;
            }
        }
        return 0;
    }

}
