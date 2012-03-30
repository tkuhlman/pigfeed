-- updates all the stats for the given day
-- Expects params LOGDIR, REPORTDIR and SITE

run -param LOGDIR=$LOGDIR web_load.pig

-- determine total number of requests and bytes served by UTC hour of day
by_hour_count = 
  FOREACH 
    (GROUP log BY FORMAT_DT('HH',datetime)) 
  GENERATE 
    $0, 
    COUNT($1) AS num_requests, 
    SUM($1.bytes) AS num_bytes
  ;

STORE by_hour_count INTO '$REPORTDIR/total_requests_bytes_per_hour';


-- top 50 ips, requests, bytes
by_ip_count = 
  FOREACH 
    (GROUP log BY FORMAT('%s', EXTRACT(remoteAddr, '(\\d+\\.\\d+\\.\\d+\\.\\d+)'))) 
  GENERATE 
    $0, 
    COUNT($1) AS num_requests, 
    SUM($1.bytes) AS num_bytes
  ;

by_ip_count_sorted = 
  -- order ip by the number of requests they make
  LIMIT (ORDER by_ip_count BY num_requests DESC) 50;

STORE by_ip_count_sorted into '$REPORTDIR/top_50_ips';
 

-- top 50 external referrers
by_referrer_count = 
  FOREACH 
    (GROUP log BY referrer)
  GENERATE 
    FLATTEN($0), 
    COUNT($1) AS num_requests
  ;

by_referrer_count_filtered = 
  -- exclude matches for site
  FILTER by_referrer_count BY NOT $0 matches 'http[s]?://$SITE.*';

by_referrer_count_sorted = 
  -- take the top 50 results
  LIMIT (ORDER by_referrer_count_filtered BY num_requests DESC) 50;

STORE by_referrer_count_sorted INTO '$REPORTDIR/top_50_external_referrers';
 
