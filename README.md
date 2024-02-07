# voltdb-tumblingwindows

A demonstration of how VoltDB can be used for various kinds of windowing tasks

## Session Windows

In session windowing we cut output records when we feel like it - we use arbitrary criteria, such as number of input records we’ve seen, total value of input records since the last output record, or elapsed time since the last output record.  This is really hard to do with any SQL dialect.

````
CREATE STREAM cc_event_stream
PARTITION ON COLUMN cardid
(cardid varchar(16) not null 
,txn_time timestamp default now
,txn_id   bigint not null
,txn_amount         decimal not null 
,txn_store bigint   not null
);

CREATE TABLE cc_event_last_20
(cardid varchar(16) not null PRIMARY KEY
,event_count           tinyint default 1 not null
,txn_amount_00         decimal default 0 not null 
,txn_amount_01         decimal default 0 not null 
,txn_amount_02         decimal default 0 not null 
,txn_amount_03         decimal default 0 not null 
,txn_amount_04         decimal default 0 not null 
,txn_amount_05         decimal default 0 not null 
,txn_amount_06         decimal default 0 not null 
,txn_amount_07         decimal default 0 not null 
,txn_amount_08         decimal default 0 not null 
,txn_amount_09         decimal default 0 not null 
,txn_amount_10         decimal default 0 not null 
,txn_amount_11         decimal default 0 not null 
,txn_amount_12         decimal default 0 not null 
,txn_amount_13         decimal default 0 not null 
,txn_amount_14         decimal default 0 not null 
,txn_amount_15         decimal default 0 not null 
,txn_amount_16         decimal default 0 not null 
,txn_amount_17         decimal default 0 not null 
,txn_amount_18         decimal default 0 not null 
,txn_amount_19         decimal default 0 not null 
,last_update_date      timestamp default NOW
,create_date           timestamp default NOW
);

PARTITION TABLE cc_event_last_20 ON COLUMN cardid;

CREATE INDEX cc_event_last_20_ix1 ON cc_event_last_20(last_update_date, cardid);

-- This can also be a STREAM….
CREATE TABLE cc_event_session_window
(report_time timestamp default now
,cardid varchar(16) not null 
,record_reason varchar(10) not null 
,txn_time timestamp default now 
,total_txn_amount         decimal not null
,last_txn_amount         decimal not null
,how_many           bigint not null
);

PARTITION TABLE cc_event_session_window ON COLUMN cardid;


public class ReportSessionWindowEvent extends VoltProcedure {

    // @formatter:off
    
    static final String ADD_COLS_TOGETHER = 
            "   (txn_amount_19  +  " 
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
                    + "   txn_amount_00)";

    public static final SQLStmt createEvent = new SQLStmt(
            "insert into cc_event_stream (cardid, txn_time "
            + ",txn_id  "
            + ",txn_amount "
            + ",txn_store ) values (?,NOW,?,?,?);");

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
            + "  , event_count = least(20,event_count+1)"
            + "  , last_update_date = NOW "
            + "WHERE cardid = ?;");
    
    public static final SQLStmt createEventTop20 = new SQLStmt(
            "insert into cc_event_last_20 (cardid, txn_amount_00) values (?,?)");

    public static final SQLStmt getWindowValues = new SQLStmt("SELECT "
            + ADD_COLS_TOGETHER
             + " total"
            + "   , event_count"
            + "   , create_date "
            + "FROM cc_event_last_20 WHERE cardid = ?; ");


    public static final SQLStmt clearEventTop20 = new SQLStmt(
            "DELETE FROM cc_event_last_20 "
          + "WHERE cardid = ?;");
 
    public static final SQLStmt recordWindowValues = new SQLStmt(         
            "INSERT INTO cc_event_session_window (report_time ,cardid ,record_reason, "
            + "total_txn_amount, last_txn_amount, how_many) " 
      + "SELECT NOW, cardid, CAST(? AS VARCHAR),"
      + ADD_COLS_TOGETHER
      + ",  txn_amount_00, event_count  "
      + "FROM cc_event_last_20 WHERE cardid = ?; ");
      
     
 // @formatter:on

    private static final long MAX_RECORDS_IN_SESSION = 20;

    public VoltTable[] run(String cardId, long txnId, double txnAmount, long storeId, int maxSessionSeconds,
            int cutrecordThresholdValue) throws VoltAbortException {

        final TimestampType cutoffDate = new TimestampType(
                new Date(this.getTransactionTime().getTime() - (maxSessionSeconds * 1000)));

        voltQueueSQL(createEvent, cardId, txnId, txnAmount, storeId);
        voltQueueSQL(updateEventTop20, txnAmount, cardId);

        VoltTable[] sessionRecords = voltExecuteSQL();

        final VoltTable updateResultTable = sessionRecords[1];

        if (updateResultTable.advanceRow() && updateResultTable.getLong("modified_tuples") == 0) {
            voltQueueSQL(createEventTop20, cardId, txnAmount);
            voltExecuteSQL();
        }

        voltQueueSQL(getWindowValues, cardId);
        final VoltTable endResultTable = voltExecuteSQL()[0];
        endResultTable.advanceRow();

        String cutRecordReason = null;

        if (endResultTable.getDecimalAsBigDecimal("total").doubleValue() >= cutrecordThresholdValue) {
            cutRecordReason = "TOTAL";
        } else if (endResultTable.getLong("event_count") >= MAX_RECORDS_IN_SESSION) {
            cutRecordReason = "COUNT";
        } else if (cutoffDate.asExactJavaDate()
                .after(endResultTable.getTimestampAsTimestamp("create_date").asExactJavaDate())) {
            cutRecordReason = "DURATION";
        }

        if (cutRecordReason != null) {
            voltQueueSQL(recordWindowValues, cutRecordReason, cardId);
            voltQueueSQL(clearEventTop20, cardId);
        }

        return voltExecuteSQL(true);

    }

}





CREATE PROCEDURE  
   PARTITION ON TABLE cc_event_stream COLUMN cardid
   FROM CLASS tumblingwindows.ReportSessionWindowEvent;  


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





CREATE PROCEDURE  
   DIRECTED
   FROM CLASS tumblingwindows.CloseStaleSessions;  
   
 
CREATE TASK stale_session_task ON SCHEDULE EVERY 1 SECONDS 
PROCEDURE CloseStaleSessions WITH (120,100)
RUN ON PARTITIONS;
````

## Hopping Windows

In a “Hopping Window” you get one output record for each arbitrary time period, hopping forward by another time period so the data overlaps.  In the example below we take a rolling  5 minute block, which we write one minute after the block finishes.

````
CREATE STREAM cc_event_stream
PARTITION ON COLUMN cardid
(cardid varchar(16) not null 
,txn_time timestamp default now
,txn_id   bigint not null
,txn_amount         decimal not null 
,txn_store bigint   not null
);


create view cc_event_by_card_by_minute as 
select cardid, truncate(MINUTE, txn_time) txn_time
     , sum(txn_amount) sum_txn_amount
     , count(*) how_many 
from cc_event_stream 
group by cardid, truncate(MINUTE, txn_time) ;

create index cc_event_by_card_by_minute_ix1 on cc_event_by_card_by_minute(txn_time, cardid);

-- This can also be a STREAM….
CREATE TABLE cc_event_hopping_window
(report_time timestamp default now
,cardid varchar(16) not null 
,avg_txn_amount         decimal not null
,total_txn_amount       decimal not null
,how_many        bigint not null
);

PARTITION TABLE cc_event_hopping_window ON COLUMN cardid;

CREATE PROCEDURE hopping_window DIRECTED AS
INSERT INTO cc_event_hopping_window (report_time,cardid, avg_txn_amount, total_txn_amount,how_many) 
SELECT NOW,cardid, sum(sum_txn_amount) / sum(how_many)  , sum(how_many), count(*)
FROM cc_event_by_card_by_minute 
WHERE txn_time BETWEEN  DATEADD(MINUTE, ?, TRUNCATE(MINUTE, NOW)) AND DATEADD(MINUTE, ?, TRUNCATE(MINUTE, NOW))
GROUP BY cardid;

CREATE TASK hopping_window_task ON SCHEDULE EVERY 1 MINUTES 
PROCEDURE hopping_window WITH (-6,-1)
RUN ON PARTITIONS;
````

## Sliding Windows

In a sliding window there is often a 1:1 ratio between input records and output records, with each output record being (say) the average of the last 20 input records.

````
CREATE STREAM cc_event_stream
PARTITION ON COLUMN cardid
(cardid varchar(16) not null 
,txn_time timestamp default now
,txn_id   bigint not null
,txn_amount         decimal not null 
,txn_store bigint   not null
);

CREATE TABLE cc_event_last_20
(cardid varchar(16) not null PRIMARY KEY
,event_count           tinyint default 1 not null
,txn_amount_00         decimal default 0 not null 
,txn_amount_01         decimal default 0 not null 
,txn_amount_02         decimal default 0 not null 
,txn_amount_03         decimal default 0 not null 
,txn_amount_04         decimal default 0 not null 
,txn_amount_05         decimal default 0 not null 
,txn_amount_06         decimal default 0 not null 
,txn_amount_07         decimal default 0 not null 
,txn_amount_08         decimal default 0 not null 
,txn_amount_09         decimal default 0 not null 
,txn_amount_10         decimal default 0 not null 
,txn_amount_11         decimal default 0 not null 
,txn_amount_12         decimal default 0 not null 
,txn_amount_13         decimal default 0 not null 
,txn_amount_14         decimal default 0 not null 
,txn_amount_15         decimal default 0 not null 
,txn_amount_16         decimal default 0 not null 
,txn_amount_17         decimal default 0 not null 
,txn_amount_18         decimal default 0 not null 
,txn_amount_19         decimal default 0 not null 
,last_update_date      timestamp default NOW
,create_date           timestamp default NOW
);

PARTITION TABLE cc_event_last_20 ON COLUMN cardid;

CREATE INDEX cc_event_last_20_ix1 ON cc_event_last_20(last_update_date, cardid);

-- This can also be a STREAM….
CREATE TABLE cc_event_sliding_window
(report_time timestamp default now
,cardid varchar(16) not null 
,txn_time timestamp default now 
,avg_txn_amount         decimal not null
,last_txn_amount         decimal not null
,how_many           bigint not null
);

PARTITION TABLE cc_event_sliding_window ON COLUMN cardid;


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

        voltQueueSQL(createEvent, cardId,txnId,txnAmount,storeId);
        voltQueueSQL(updateEventTop20, txnAmount, cardId);
		
		VoltTable[] sessionRecords = voltExecuteSQL();
		
		final VoltTable updateResultTable  = sessionRecords[1];
		
		if (updateResultTable.advanceRow() && updateResultTable.getLong("modified_tuples") == 0) {
		       voltQueueSQL(createEventTop20, cardId,txnAmount);
		}

		
		voltQueueSQL(recordWindowValues, cardId);
        return voltExecuteSQL(true);
		
		}

	
}



CREATE PROCEDURE  
   PARTITION ON TABLE cc_event_stream COLUMN cardid
   FROM CLASS tumblingwindows.ReportSlidingWindowEvent;  
````
## Tumbling Windows

In a “Tumbling Window” you get one output record for each arbitrary time period.

As we mentioned at the start of this doc Views that use truncate are very useful, but because the ‘official’ truncate function stores far more rows than we may need we desire an arbitrary truncate function, one that allows us to map an input date to an arbitrary length time period, relative to an arbitrary starting date.

```
-- This could have MIGRATE/TTL enabled, but pay attention 
-- to when MIGRATE/TTL happens, as it needs to be after the record
-- will no longer be written to
CREATE TABLE cc_event_arbitrary_tumbling_window
(report_time timestamp not null
,cardid varchar(16) not null 
,total_txn_amount       decimal not null
,how_many        bigint not null
,primary key (cardid,report_time)
);

PARTITION TABLE cc_event_arbitrary_tumbling_window ON COLUMN cardid;



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

    

    
}





CREATE FUNCTION  arbitraryTruncateWithBaseTime FROM METHOD javafunctions.ArbitraryTruncate.arbitraryTruncateWithBaseTime;


public class ReportArbitraryTumblingWindowEvent extends VoltProcedure {

    // @formatter:off

    public static final SQLStmt createEvent = new SQLStmt(
            "insert into cc_event_arbitrary_tumbling_window (report_time"
            + ",cardid "
            + ",total_txn_amount "
            + ",how_many ) values (arbitraryTruncateWithBaseTime(?,?,?),?,?,1);");

    public static final SQLStmt updateEvent = new SQLStmt(
            "update cc_event_arbitrary_tumbling_window "
            + "set total_txn_amount = total_txn_amount + ?"
            + "  , how_many = how_many + 1"
            + "where cardid = ? "
            + "and   report_time = arbitraryTruncateWithBaseTime(?,?,?);");

    // @formatter:on

    public VoltTable[] run(String cardId, long txnId, double txnAmount, long storeId, long intervalMs,
            TimestampType knownBoundary) throws VoltAbortException {

        // Attempt to update existing record. 
        voltQueueSQL(updateEvent, txnAmount, cardId, this.getTransactionTime(), intervalMs, knownBoundary);

        final VoltTable updateResultTable = voltExecuteSQL()[0];

        if (updateResultTable.advanceRow() && updateResultTable.getLong("modified_tuples") == 0) {
            // No Record found, so we create one.
            voltQueueSQL(createEvent, this.getTransactionTime(), intervalMs, knownBoundary, cardId, txnAmount);
        }

        return voltExecuteSQL(true);

    }

}




````

## Arbitrary Tumbling Windows

Suppose we need to do some kind of window based logic, but the rules are both complicated and changeable. We can’t easily create new views on the fly, so what do we do?


````


-- 
-- Used by Arbitrary aggregation example
--
CREATE STREAM cc_agg_attribute_stream
PARTITION ON COLUMN cardid
(cardid varchar(16) not null 
,txn_time timestamp default now
,txn_id   varchar(16) not null
,txn_amount         decimal not null 
,txn_kind varchar(16) not null 
,txn_tag1    varchar(10)   not null
,txn_tag2    varchar(10)   
,txn_tag3    varchar(10)   
,txn_tag4    varchar(10)   
,txn_tag1_value    bigint   
,txn_tag2_value     bigint   
,txn_tag3_value     bigint   
,txn_tag4_value     bigint   
);

-- 
-- Used by Arbitrary aggregation example
--
CREATE VIEW cc_agg_attribute_table_totals AS
SELECT cardid
     , txn_kind
     ,truncate(MINUTE,txn_time) txn_time
     ,txn_tag1
     ,txn_tag2
     ,txn_tag3
     ,txn_tag4
     , count(*) how_many
     , sum(txn_tag1_value) txn_tag1_value
     , sum(txn_tag2_value) txn_tag2_value
     , sum(txn_tag3_value) txn_tag3_value
     , sum(txn_tag4_value) txn_tag4_value
     , sum(txn_amount) how_much
FROM cc_agg_attribute_stream
GROUP BY cardid
       ,txn_kind
       ,truncate(MINUTE,txn_time)
       ,txn_tag1
       ,txn_tag2
       ,txn_tag3
       ,txn_tag4;
       
CREATE TABLE cc_agg_alert
(cardid varchar(16) not null 
,alert_code varchar(16) 
,first_seen_time timestamp default now
,txn_time timestamp default now
,message varchar(1024) not null
,primary key (cardid,alert_code));

PARTITION TABLE cc_agg_alert ON COLUMN cardid;








And to process it:


package arbitraryattributes;

import java.math.BigDecimal;
import java.util.Date;

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
            String txn_tg3, long txn_tag3_value, String txn_tag4, long txn_tag4_value) throws VoltAbortException {

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




The key point is that we’re not pretending that our view and stream are accurate representations of anything. Their contents are defined by changeable logic. Each chunk of logic is a class that implements ArbitraryEventProcessor:


   ArbitraryEventProcessor[] processorArray = { new HighValueTransactionObserver(100,300) 
            , new PhonePurchaseTransactionObserver(1)};





In the example above we have a ‘HighValueTransactionObserver’ and a ‘PhonePurchaseTransactionObserver’. They each do what the name suggests. An instance of ArbitraryEventProcessor has two main methods:

noteDetailsOfEvent

This is told what has just happened, and can insert between zero and many records into cc_agg_attribute_stream, which drives the view. What we insert and why we insert it it the developer’s business.

As part of event processing we call ‘noteDetailsOfEvent’ for each instance of ArbitraryEventProcessor we have in our array. We then query all the data for the user, and pass it to ‘raiseAlertsIfNeeded’.

raiseAlertsIfNeeded

This is given two VoltTables, which contain raw and summarized view data for a card. From this you can count the outstanding data and decide whether to raise an alert.

IMPORTANT POINT: I can change ArbitraryEventProcessors or add new ones without disrupting activity.


````
