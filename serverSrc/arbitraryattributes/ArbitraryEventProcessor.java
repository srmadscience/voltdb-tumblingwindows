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

/**
 * Interface that allows you to add or change business functionality at will. Each class that implements this gets a handle to
 * EventReporterInterface, which is a set of methods in the parent stored procedure for storing data and raising alerts. 
 *
 */
public interface ArbitraryEventProcessor {
    
    /**
     * 
     * A new event has arrived, and we need to decide what to do with it. We can use the methods exposed by 
     * EventReporterInterface to do this. This method is called once for each ArbitraryEventProcessor.
     * 
     * @param theReporter
     * @param cardid
     * @param txn_time
     * @param txn_id
     * @param txn_amount
     * @param txn_kind
     * @param txn_tag1
     * @param txn_tag1_value
     * @param txn_tag2
     * @param txn_tag2_value
     * @param txn_tag3
     * @param txn_tag3_value
     * @param txn_tag4
     * @param txn_tag4_value
     * @return number of records created
     */
    public int noteDetailsOfEvent(EventReporterInterface theReporter, String cardid, TimestampType txn_time, String txn_id, BigDecimal txn_amount,
            String txn_kind, String txn_tag1, long txn_tag1_value, String txn_tag2, long txn_tag2_value,
            String txn_tag3, long txn_tag3_value, String txn_tag4, long txn_tag4_value);

    /**
     *  
     * 
     * @param theReporter
     * @param totalsTable
     * @param totalsByMinuteTable
     * @return number of alerts raised
     */
    public int raiseAlertsIfNeeded(EventReporterInterface theReporter, VoltTable totalsTable, VoltTable totalsByMinuteTable);

}
