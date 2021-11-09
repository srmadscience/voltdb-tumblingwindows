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
 *
 */
public class CloseStaleSessions extends VoltProcedure {

    // @formatter:off

    public static final SQLStmt getStaleSessions = new SQLStmt(
            "SELECT last_update_date, cardid  "
            + "FROM cc_event_last_20 "
          + "ORDER BY last_update_date, cardid "
          + "LIMIT ?;");

    public static final SQLStmt clearEventTop20 = new SQLStmt(
            "DELETE FROM cc_event_last_20 "
          + "WHERE cardid = ?;");

 
    public static final SQLStmt recordWindowValues = new SQLStmt(         
            "INSERT INTO cc_event_session_window (report_time, cardid, record_reason, total_txn_amount, last_txn_amount, how_many) " 
      + "SELECT last_update_date, cardid, 'STALE', (txn_amount_19  +  " 
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
      + "   txn_amount_00) "
      + ",  txn_amount_00 , event_count "
      + "FROM cc_event_last_20 WHERE cardid = ?; ");
      
     
 // @formatter:on

    public VoltTable[] run(int maxSessionSeconds, int sessionBatchSize) throws VoltAbortException {


        final TimestampType cutoffDate = new TimestampType(
                new Date(this.getTransactionTime().getTime() - (maxSessionSeconds * 1000)));
        
        voltQueueSQL(getStaleSessions,sessionBatchSize);
        final VoltTable sessionRecords = voltExecuteSQL()[0];

        while (sessionRecords.advanceRow()) {

            String cardId = sessionRecords.getString("cardid");
            TimestampType lastUpdateDate = sessionRecords.getTimestampAsTimestamp("last_update_date");

            if (cutoffDate.asExactJavaDate().before(lastUpdateDate.asExactJavaDate())) {
                break;
            }

            voltQueueSQL(recordWindowValues, cardId);
            voltQueueSQL(clearEventTop20, cardId);

        }

        return voltExecuteSQL(true);

    }

}
