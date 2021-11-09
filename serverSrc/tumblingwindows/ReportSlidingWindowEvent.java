package tumblingwindows;

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


/**
 * Store an event for a single card, but compute rolling stats for the last 20.
 */
public class ReportSlidingWindowEvent extends VoltProcedure {

	// @formatter:off

	public static final SQLStmt updateEventTop20 = new SQLStmt(
			"UPDATE cc_event_last_20 "
			+ "SET txn_amount_19 = txn_amount_18"
            + "  , txn_amount_18 = txn_amount_17 "
            + "  , txn_amount_17 = txn_amount_16 "
            + "  , txn_amount_16 = txn_amount_15 "
            + "  , txn_amount_15 = txn_amount_14 "
            + "  , txn_amount_14 = txn_amount_13"
            + "  , txn_amount_13 = txn_amount_12 "
            + "  , txn_amount_12 = txn_amount_11 "
            + "  , txn_amount_11 = txn_amount_10 "
            + "  , txn_amount_10 = txn_amount_09 "
            + "  , txn_amount_09 = txn_amount_08 "
            + "  , txn_amount_08 = txn_amount_07 "
            + "  , txn_amount_07 = txn_amount_06 "
            + "  , txn_amount_06 = txn_amount_05 "
            + "  , txn_amount_05 = txn_amount_04 "
            + "  , txn_amount_04 = txn_amount_03 "
            + "  , txn_amount_03 = txn_amount_02 "
            + "  , txn_amount_02 = txn_amount_01 "
            + "  , txn_amount_01 = txn_amount_00 "
			+ "  , txn_amount_00 = ?"
			+ "  , event_count = least(20,event_count+1) "
            + "  , last_update_date = NOW "
			+ "WHERE cardid = ?;");

    public static final SQLStmt createEventTop20 = new SQLStmt(
            "insert into cc_event_last_20 (cardid, txn_amount_00) values (?,?)");

    public static final SQLStmt createEvent = new SQLStmt(
            "insert into cc_event_stream (cardid, txn_time "
            + ",txn_id  "
            + ",txn_amount "
            + ",txn_store ) values (?,NOW,?,?,?);");

	   public static final SQLStmt recordWindowValues = new SQLStmt(         
	         "INSERT INTO cc_event_sliding_window (report_time ,cardid ,avg_txn_amount, last_txn_amount, how_many) " 
	   + "SELECT NOW, cardid, (txn_amount_19  +  " 
       + "   txn_amount_18 +  "
       + "   txn_amount_17 +  "
       + "   txn_amount_16 +  "
       + "   txn_amount_15 +  "
       + "   txn_amount_14 + "
       + "   txn_amount_13 +  "
       + "   txn_amount_12 +  "
       + "   txn_amount_11 +  "
       + "   txn_amount_10 +  "
       + "   txn_amount_09 +  "
       + "   txn_amount_08 +  "
       + "   txn_amount_07 +  "
       + "   txn_amount_06 +  "
       + "   txn_amount_05 +  "
       + "   txn_amount_04 +  "
       + "   txn_amount_03 +  "
       + "   txn_amount_02 +  "
       + "   txn_amount_01 +  "
       + "   txn_amount_00) / event_count"
       + ",  txn_amount_00, event_count "
       + "FROM cc_event_last_20 WHERE cardid = ?; ");
	   
	public VoltTable[] run(String cardId, long txnId , double txnAmount ,long storeId ) throws VoltAbortException {

	    // Recorf event and update rolling top 20
        voltQueueSQL(createEvent, cardId,txnId,txnAmount,storeId);
        voltQueueSQL(updateEventTop20, txnAmount, cardId);
		
		VoltTable[] sessionRecords = voltExecuteSQL();
		
		final VoltTable updateResultTable  = sessionRecords[1];
		
		if (updateResultTable.advanceRow() && updateResultTable.getLong("modified_tuples") == 0) {
		       // No record existed, so create one
		       voltQueueSQL(createEventTop20, cardId,txnAmount);
		}

		// Write output record
		voltQueueSQL(recordWindowValues, cardId);
        return voltExecuteSQL(true);
		
		}

	
}
