-- Load the processed/combined web log files in the directory LOGDIR
log = LOAD '$LOGDIR' AS (
    remoteAddr: chararray,
    method: chararray,
    uri: chararray,
    status: int,
    referrer: chararray, 
    userAgent: chararray,
    timeTaken: int,
    datetime: chararray,
    bytes: int
    );

