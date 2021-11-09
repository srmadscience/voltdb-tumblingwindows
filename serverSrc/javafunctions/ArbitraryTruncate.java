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

public class ArbitraryTruncate {
    
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

    public TimestampType arbitraryTruncate (TimestampType value, long intervalMs) throws VoltAbortException {
        
        final TimestampType knownBoundary = new TimestampType(new Date(0));
        
        return arbitraryTruncateWithBaseTime(value, intervalMs, knownBoundary);
         
     }

   

    
}
