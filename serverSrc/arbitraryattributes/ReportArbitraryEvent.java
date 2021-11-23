package arbitraryattributes;

import java.math.BigDecimal;
import java.util.Date;

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

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

/**
 * Procedure to update running totals for a card for an arbitrary length time
 * periods
 */
public class ReportArbitraryEvent extends VoltProcedure implements EventReporterInterface {

    // @formatter:off

    public static final SQLStmt createEvent = new SQLStmt(
            "insert into cc_agg_attribute_stream ("
            + "cardid    "    
            + ",txn_time  "
            + ",txn_id      "
            + ",txn_amount             "
            + ",txn_kind     "   
            + ",txn_tag1         "
            + ",txn_tag2         "
            + ",txn_tag3         "
            + ",txn_tag4         "
            + ",txn_tag1_value         "
            + ",txn_tag2_value          "
            + ",txn_tag3_value          "
            + ",txn_tag4_value          "
            + " ) values (?,?,?,?,?,?,?,?,?,?,?,?,?);");

    public static final SQLStmt deleteOldRecord = new SQLStmt(
            "DELETE FROM cc_agg_attribute_table_totals "
            + "WHERE cardid = ? "
            + "AND   txn_kind = ?"
            + "AND   txn_time = ?;");

    public static final SQLStmt raiseAlert = new SQLStmt(
            "upsert into cc_agg_alert ("
            + "cardid "
            + ",alert_code   "    
            + ",txn_time  "
            + ",message ) values (?,?,?,?);");

    public static final SQLStmt getOldest = new SQLStmt(
            "SELECT txn_time  "
            + "FROM cc_agg_attribute_table_totals "
            + "WHERE cardid = ? "
            + "and    txn_kind = ?"
            + "ORDER BY txn_time LIMIT 2;");

    public static final SQLStmt getTotals = new SQLStmt(
            "SELECT  cardid"
            + "     ,txn_kind"
            + "     ,txn_tag1"
            + "     ,txn_tag2"
            + "     ,txn_tag3"
            + "     ,txn_tag4"
            + "     , count(*) how_many"
            + "     , sum(txn_tag1_value) txn_tag1_value"
            + "     , sum(txn_tag2_value) txn_tag2_value"
            + "     , sum(txn_tag3_value) txn_tag3_value"
            + "     , sum(txn_tag4_value) txn_tag4_value"
            + "     , sum(how_much) how_much "
            + "from cc_agg_attribute_table_totals "
            + "WHERE cardid = ? "
            + "GROUP BY cardid, txn_kind, txn_tag1, txn_tag2, txn_tag3, txn_tag4 "
            + "ORDER BY cardid, txn_kind, txn_tag1, txn_tag2, txn_tag3, txn_tag4;");

    public static final SQLStmt getTotalsByMinute = new SQLStmt(
            "SELECT  cardid"
            + "     , txn_kind"
            + "     , txn_time "
            + "     ,txn_tag1"
            + "     ,txn_tag2"
            + "     ,txn_tag3"
            + "     ,txn_tag4"
            + "     , count(*) how_many"
            + "     , sum(txn_tag1_value) txn_tag1_value"
            + "     , sum(txn_tag2_value) txn_tag2_value"
            + "     , sum(txn_tag3_value) txn_tag3_value"
            + "     , sum(txn_tag4_value) txn_tag4_value"
            + "     , sum(how_much) how_much "
            + "from cc_agg_attribute_table_totals "
            + "WHERE cardid = ? "
            + "GROUP BY cardid, txn_time, txn_kind, txn_tag1, txn_tag2, txn_tag3, txn_tag4 "
            + "ORDER BY cardid, txn_time, txn_kind, txn_tag1, txn_tag2, txn_tag3, txn_tag4 ;");

  
    // @formatter:on

    ArbitraryEventProcessor[] processorArray = { new HighValueTransactionObserver(100,300) 
            , new PhonePurchaseTransactionObserver(1)};

    /**
     * Store an event for a single card. Generate an output record based on Total
     * value, number of events or time sicne we last wrote a record.
     * 
     * @param cardId
     * @param txnId
     * @param txnAmount
     * @param storeId
     * @param intervalMs
     * @param knownBoundary
     * @return
     * @throws VoltAbortException
     */
    public VoltTable[] run( String cardid, TimestampType txn_time, String txn_id, BigDecimal txn_amount,
            String txn_kind, String txn_tag1, long txn_tag1_value, String txn_tag2, long txn_tag2_value,
            String txn_tag3, long txn_tag3_value, String txn_tag4, long txn_tag4_value) throws VoltAbortException {

        // Tell all our event processors about this event so they can update their totals...
        for (int i = 0; i < processorArray.length; i++) {
            processorArray[i].noteDetailsOfEvent(this, cardid, txn_time, txn_id, txn_amount, txn_kind, txn_tag1, txn_tag1_value, txn_tag2, txn_tag2_value, txn_tag3, txn_tag3_value, txn_tag4, txn_tag4_value);
        }

        voltQueueSQL(getTotals, cardid);
        voltQueueSQL(getTotalsByMinute, cardid);
        
        final VoltTable[] updateResultTables = voltExecuteSQL();   
        final VoltTable totalsTable = updateResultTables[updateResultTables.length -2];
        final VoltTable totalsByMinuteTable = updateResultTables[updateResultTables.length -1];

        for (int i = 0; i < processorArray.length; i++) {
            
            totalsTable.resetRowPosition();
            totalsByMinuteTable.resetRowPosition(); 
            processorArray[i].raiseAlertsIfNeeded(this,  totalsTable,  totalsByMinuteTable);
            
        }
        
        return voltExecuteSQL(true);

    }

    public void reportEvent(String cardid, String txn_time, String expire_time, String txn_id, BigDecimal txn_amount,
            String txn_kind, String txn_tag1, String txn_tag2, String txn_tag3, String txn_tag4, long txn_tag1_value,
            long txn_tag2_value, long txn_tag3_value, long txn_tag4_value) {

        // Attempt to update existing record that contains running total.
        voltQueueSQL(createEvent, cardid, txn_time, expire_time, txn_id, txn_amount, txn_kind, txn_tag1, txn_tag2,
                txn_tag3, txn_tag4, txn_tag1_value, txn_tag2_value, txn_tag3_value, txn_tag4_value);

    }

    @Override
    public void raiseAlert(String cardid,String code, String message) {

        voltQueueSQL(raiseAlert, cardid, code, this.getTransactionTime(), message);
        voltExecuteSQL();

    }

    @Override
    public void deleteOldEvents(String cardid, String txn_kind, int max_age_minutes) {
        
        final Date purgeDate  = new Date(this.getTransactionTime().getTime() - (1000 * max_age_minutes * 24));
        
        voltQueueSQL(getOldest, cardid, txn_kind);
        
        VoltTable oldestRecords = voltExecuteSQL()[0];
        
        while (oldestRecords.advanceRow()) {
            
            TimestampType recordDate = oldestRecords.getTimestampAsTimestamp("txn_time");
            
            if (recordDate.asApproximateJavaDate().before(purgeDate)) {
                voltQueueSQL(deleteOldRecord, cardid, txn_kind, recordDate);
            } else {
                break;
            }
            
        }
        
        
        
        
        voltExecuteSQL();

    }

    @Override
    public void reportEvent(String cardid, TimestampType txn_time, String txn_id, BigDecimal txn_amount,
            String txn_kind, String txn_tag1, long txn_tag1_value, String txn_tag2, long txn_tag2_value,
            String txn_tag3, long txn_tag3_value, String txn_tag4, long txn_tag4_value) {
        
        voltQueueSQL(createEvent, cardid, txn_time, txn_id, txn_amount, txn_kind, txn_tag1, txn_tag2, txn_tag3,
                txn_tag4, txn_tag1_value, txn_tag2_value, txn_tag3_value, txn_tag4_value);
        voltExecuteSQL();
       
    }

}
