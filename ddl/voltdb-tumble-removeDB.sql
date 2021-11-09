
file -inlinebatch END_OF_BATCH

DROP TASK stale_session_task IF EXISTS;
DROP TASK tumbling_window_task IF EXISTS;
DROP TASK hopping_window_task IF EXISTS;

DROP PROCEDURE ReportSlidingWindowEvent IF EXISTS;

DROP PROCEDURE ReportSessionWindowEvent IF EXISTS;

DROP PROCEDURE ReportArbitraryTumblingWindowEvent IF EXISTS;

DROP PROCEDURE CloseStaleSessions IF EXISTS;

DROP PROCEDURE hopping_window IF EXISTS;

DROP PROCEDURE tumbling_window IF EXISTS;

DROP VIEW cc_event_by_card_by_minute IF EXISTS;

DROP VIEW cc_event_by_card_by_5minutes IF EXISTS;

DROP VIEW cc_event_session_window_reasons IF EXISTS;

DROP TABLE cc_event_tumbling_window IF EXISTS;

DROP TABLE cc_event_arbitrary_tumbling_window IF EXISTS;

DROP TABLE cc_event_hopping_window IF EXISTS;

DROP TABLE cc_event_sliding_window IF EXISTS;

DROP TABLE cc_event_session_window IF EXISTS;

DROP TABLE cc_event_last_20 IF EXISTS;

DROP STREAM cc_event_stream IF EXISTS;

DROP TABLE cc_event_table IF EXISTS;

DROP FUNCTION GREATEST IF EXISTS;
DROP FUNCTION LEAST IF EXISTS;
DROP FUNCTION arbitraryTruncateWithBaseTime IF EXISTS;
DROP FUNCTION arbitraryTruncate             IF EXISTS;

END_OF_BATCH
