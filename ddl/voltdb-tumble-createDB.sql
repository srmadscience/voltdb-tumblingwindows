


file voltdb-tumble-removeDB.sql;

LOAD CLASSES ../jars/voltdb-tumble.jar;


file -inlinebatch END_OF_BATCH


CREATE FUNCTION GREATEST FROM METHOD javafunctions.MathComparators.greatest;
CREATE FUNCTION LEAST    FROM METHOD javafunctions.MathComparators.least;
CREATE FUNCTION  arbitraryTruncateWithBaseTime FROM METHOD javafunctions.ArbitraryTruncate.arbitraryTruncateWithBaseTime;
CREATE FUNCTION  arbitraryTruncate             FROM METHOD javafunctions.ArbitraryTruncate.arbitraryTruncate;

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

-- This can also be a STREAM...
CREATE TABLE cc_event_tumbling_window
(report_time timestamp default now
,cardid varchar(16) not null 
,avg_txn_amount         decimal not null
,total_txn_amount       decimal not null
,how_many        bigint not null
);

PARTITION TABLE cc_event_tumbling_window ON COLUMN cardid;

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


-- This can also be a STREAM...
CREATE TABLE cc_event_hopping_window
(report_time timestamp default now
,cardid varchar(16) not null 
,avg_txn_amount         decimal not null
,total_txn_amount       decimal not null
,how_many        bigint not null
);

PARTITION TABLE cc_event_hopping_window ON COLUMN cardid;


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

-- This can also be a STREAM...
CREATE TABLE cc_event_sliding_window
(report_time timestamp default now
,cardid varchar(16) not null 
,txn_time timestamp default now 
,avg_txn_amount         decimal not null
,last_txn_amount         decimal not null
,how_many           bigint not null
);

PARTITION TABLE cc_event_sliding_window ON COLUMN cardid;

-- This can also be a STREAM...
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

CREATE VIEW cc_event_session_window_reasons AS
SELECT record_reason, count(*) how_many
FROM cc_event_session_window
GROUP BY record_reason;



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

CREATE PROCEDURE tumbling_window DIRECTED AS
INSERT INTO cc_event_tumbling_window (report_time,cardid,  avg_txn_amount, total_txn_amount, how_many) 
SELECT NOW,cardid,sum(sum_txn_amount) / sum(how_many) , sum(how_many), count(*)
FROM cc_event_by_card_by_minute 
WHERE txn_time BETWEEN  DATEADD(MINUTE, ?, TRUNCATE(MINUTE, NOW)) AND DATEADD(MINUTE, ?, TRUNCATE(MINUTE, NOW))
GROUP BY cardid;

CREATE TASK tumbling_window_task ON SCHEDULE EVERY 5 MINUTES 
PROCEDURE tumbling_window WITH (-6,-1)
RUN ON PARTITIONS;

CREATE PROCEDURE hopping_window DIRECTED AS
INSERT INTO cc_event_hopping_window (report_time,cardid, avg_txn_amount, total_txn_amount,how_many) 
SELECT NOW,cardid, sum(sum_txn_amount) / sum(how_many)  , sum(how_many), count(*)
FROM cc_event_by_card_by_minute 
WHERE txn_time BETWEEN  DATEADD(MINUTE, ?, TRUNCATE(MINUTE, NOW)) AND DATEADD(MINUTE, ?, TRUNCATE(MINUTE, NOW))
GROUP BY cardid;

CREATE TASK hopping_window_task ON SCHEDULE EVERY 1 MINUTES 
PROCEDURE hopping_window WITH (-6,-1)
RUN ON PARTITIONS;

CREATE PROCEDURE  
   PARTITION ON TABLE cc_event_stream COLUMN cardid
   FROM CLASS tumblingwindows.ReportSlidingWindowEvent;  

CREATE PROCEDURE  
   PARTITION ON TABLE cc_event_stream COLUMN cardid
   FROM CLASS tumblingwindows.ReportSessionWindowEvent;  
   
CREATE PROCEDURE  
   PARTITION ON TABLE cc_event_stream COLUMN cardid
   FROM CLASS tumblingwindows.ReportArbitraryTumblingWindowEvent;
   
CREATE PROCEDURE
    PARTITION ON TABLE cc_event_stream COLUMN cardid
FROM CLASS arbitraryattributes.ReportArbitraryEvent;

CREATE PROCEDURE  
   DIRECTED
   FROM CLASS tumblingwindows.CloseStaleSessions;   
 
CREATE TASK stale_session_task ON SCHEDULE EVERY 1 SECONDS 
PROCEDURE CloseStaleSessions WITH (120,100)
RUN ON PARTITIONS;

END_OF_BATCH




