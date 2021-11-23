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
import org.voltdb.types.TimestampType;

/**
 * Procedure to update running totals for a card for an arbitrary length time periods
 */
public class ReportArbitraryAtrributeEvent extends VoltProcedure {

    // @formatter:off

    public static final SQLStmt createEvent = new SQLStmt(
            "insert into cc_event_arbitrary_tumbling_window (report_time"
            + ",cardid "
            + ",total_txn_amount "
            + ",how_many ) values (arbitraryTruncateWithBaseTime(?,?,?),?,?,1);");

    public static final SQLStmt updateEvent = new SQLStmt(
            "update cc_event_arbitrary_tumbling_window "
            + "set total_txn_amount = total_txn_amount + ?"
            + "  , how_many = how_many + 1 "
            + "where cardid = ? "
            + "and   report_time = arbitraryTruncateWithBaseTime(?,?,?);");

    // @formatter:on

    /**
     * Store an event for a single card. Generate an output record
     * based on Total value, number of events or time sicne we last
     * wrote a record.
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
    public VoltTable[] run(String cardId, long txnId, double txnAmount, long storeId, long intervalMs,
            TimestampType knownBoundary) throws VoltAbortException {

        // Attempt to update existing record that contains running total. 
        voltQueueSQL(updateEvent, txnAmount, cardId, this.getTransactionTime(), intervalMs, knownBoundary);

        final VoltTable updateResultTable = voltExecuteSQL()[0];

        if (updateResultTable.advanceRow() && updateResultTable.getLong("modified_tuples") == 0) {
            // No Record found, so we create one.
            voltQueueSQL(createEvent, this.getTransactionTime(), intervalMs, knownBoundary, cardId, txnAmount);
        }

        return voltExecuteSQL(true);

    }

}
