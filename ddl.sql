-- Table that stores values that are timestamped and
-- are partitioned by UUID.
CREATE TABLE opencalls
(
  call_id BIGINT NOT NULL,
  agent_id INTEGER NOT NULL,
  phone_no VARCHAR(20 BYTES) NOT NULL,
  start_ts TIMESTAMP DEFAULT NULL,
  end_ts TIMESTAMP DEFAULT NULL,
  PRIMARY KEY (call_id, agent_id, phone_no)
);
-- Partition this table to get parallelism.
PARTITION TABLE opencalls ON COLUMN phone_no;

CREATE TABLE completedcalls
(
  call_id BIGINT NOT NULL,
  agent_id INTEGER NOT NULL,
  phone_no VARCHAR(20 BYTES) NOT NULL,
  start_ts TIMESTAMP NOT NULL,
  end_ts TIMESTAMP NOT NULL,
  duration INTEGER NOT NULL,
  PRIMARY KEY (call_id, agent_id, phone_no),
  LIMIT PARTITION ROWS 82500
    EXECUTE (DELETE FROM completedcalls
             WHERE end_ts < dateadd(minute, -5, now)
             ORDER BY end_ts, call_id, agent_id, phone_no LIMIT 1500)
);
PARTITION TABLE completedcalls ON COLUMN phone_no;

-- Ordered index on timestamp value allows for quickly finding timestamp
-- values as well as quickly finding rows by offset.
-- Used by all 4 of the deleting stored procedures.
CREATE INDEX end_ts_index ON completedcalls (end_ts);

-- Update classes from jar to that server will know about classes but not procedures yet.
LOAD CLASSES callcenter-procs.jar;

-- stored procedures
CREATE PROCEDURE PARTITION ON TABLE opencalls COLUMN phone_no FROM CLASS callcenter.BeginCall;
CREATE PROCEDURE PARTITION ON TABLE opencalls COLUMN phone_no FROM CLASS callcenter.EndCall;
--CREATE PROCEDURE FROM CLASS callcenter.StandardDev;
