-- Loads the latest log file specified in INPUT
-- Expects params INPUT, LOGDIR, NAME, SITE, TMPDIR

--copy the file from the filesystem to hdfs then load it
fs -copyFromLocal $INPUT $TMPDIR/$NAME
raw_log = LOAD '$TMPDIR/$NAME' USING TextLoader AS (line:chararray);

-- for each weblog string convert the weblog string into a 
-- structure with named fields matching the realgo log pattern
-- LogFormat "%V %h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\" %D" realgo
-- Note that if the protocol is not specified the regex fails, the
-- only client I know of that does this is the coyote
rg_log_base = 
  FOREACH 
    raw_log
  GENERATE   
    FLATTEN ( 
      EXTRACT( 
        line, 
        '^(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+.(\\S+\\s+\\S+).\\s+"(\\S+)\\s+(.+?)\\s+(HTTP[^"]+)"\\s+(\\S+)\\s+(\\S+)\\s+"([^"]*)"\\s+"([^"]+)"\\s+(.*)$'
      )
    ) 
    AS (
      serverName: chararray,
      remoteAddr: chararray,
      remoteLogname: chararray,
      user: chararray,
      time: chararray, 
      method: chararray,
      uri: chararray,
      proto: chararray,
      status: int,
      bytes_string: chararray,
      referrer: chararray, 
      userAgent: chararray,
      timeTaken: int
    )
  ;

-- For EC2 boxes where the log format is
-- "%V %h \"%{X-Forwarded-For}i\" %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\" %D"
-- the regex should be:
--        '^(\\S+)\\s+\\S+\\s+"(\\S+),\\s+\\S+"\\s+(\\S+)\\s+(\\S+)\\s+.(\\S+\\s+\\S+).\\s+"(\\S+)\\s+(.+?)\\s+(HTTP[^"]+)"\\s+(\\S+)\\s+(\\S+)\\s+"([^"]*)"\\s+"([^"]+)"\\s+(.*)$'

-- We only need the site we are concerned with
rg_log_filtered = FILTER rg_log_base BY (serverName matches '$SITE');

-- drop uneeded fields and convert from string values to typed values such as date_time and integers
log = 
  FOREACH 
    rg_log_filtered 
  GENERATE 
    remoteAddr,
    method,
    uri,
    status,
    referrer, 
    userAgent,
    timeTaken,
    DATE_TIME(time, 'dd/MMM/yyyy:HH:mm:ss Z', 'UTC') as datetime, 
    (int)REPLACE(bytes_string, '-', '0')          as bytes
  ;

STORE log INTO '$LOGDIR/$NAME';
