package javafunctions;

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

import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.types.TimestampType;

/**
 * This is a VoltDB function that allows you to truncate TimeStamps to arbitrarily long 
 * time periods, relative to an arbitary start point. For example, I can round down all 
 * dates to 5 minute blocks, starting at 27 seconds past the minute.
 * <p>
 * While this seems esoteric, it's reallty useful if you need to group time based data
 * by arbitrary intervals.
 *
 */
public class ArbitraryTruncate {
    
    /**
     * Truncate an input TimestampType to an arbitrarily long time period, relative
     * to a known starting date. 
     * <p>
     * For example:
     * <p>
     * arbitraryTruncateWithBaseTime('10-Jan-21 15:21:37',300000,'1-Jan-21 00:00:27') 
     * will return '10-Jan-21 15:20:27', because it's working with 5 minute (300,000ms) blocks, starting at 27 seconds 
     * past midnight.
     * 
     * @param value - The value you wish to truncate
     * @param intervalMs - Length of your arbitrary time period in ms
     * @param knownBoundary - An arbitrary historical date when this set of time periods started
     * @return A new timestamp
     * @throws VoltAbortException - if any of the input values are null or meaningless
     */
    public TimestampType arbitraryTruncateWithBaseTime (TimestampType value, long intervalMs, TimestampType knownBoundary) throws VoltAbortException {
        
        if (intervalMs == Long.MIN_VALUE) {
            throw new VoltAbortException("Interval can not be null");
        }
        
        if (intervalMs == 0) {
            throw new VoltAbortException("Interval can not be zero");
        }
        
        if (intervalMs < 0) {
            throw new VoltAbortException("Interval can not be negative");
        }
        
        if (knownBoundary == null) {
            throw new VoltAbortException("knownBoundary can not be null");
        }
        
        if (value == null) {
            throw new VoltAbortException("value can not be null");
        }
        
        if (knownBoundary.asExactJavaDate().after(value.asExactJavaDate())) {
            throw new VoltAbortException("knownBoundary of " + knownBoundary.toString() + " can not be after value of " + value.toString());
        }
        

        // Calculate offset
        final long knownBoundaryValueInMs = knownBoundary.asApproximateJavaDate().getTime();
        final long relativeValueInMs = value.asApproximateJavaDate().getTime() - knownBoundaryValueInMs;
        final long intervals = relativeValueInMs / intervalMs;
        final long adjustedRelativeValueInMs = knownBoundaryValueInMs + (intervals * intervalMs);
        final TimestampType newValue = new TimestampType(new Date(adjustedRelativeValueInMs));
         
        return newValue;
         
     }

    /**
     * Truncate an input TimestampType to an arbitrarily long time period, relative
     * to midnight. 
     * <p>
     * For example:
     * <p>
     * arbitraryTruncateWithBaseTime('10-Jan-21 15:21:37',300000) 
     * will return '10-Jan-21 15:20:00', because it's working with 5 minute (300,000ms) blocks.
     * 
     * @param value - The value you wish to truncate
     * @param intervalMs - Length of your arbitrary time period in ms
     * @return A new timestamp
     * @throws VoltAbortException - if any of the input values are null or meaningless
     */
   public TimestampType arbitraryTruncate (TimestampType value, long intervalMs) throws VoltAbortException {
        
        final TimestampType knownBoundary = new TimestampType(new Date(0));
        
        return arbitraryTruncateWithBaseTime(value, intervalMs, knownBoundary);
         
     }

   

    
}
