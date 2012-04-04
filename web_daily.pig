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

-- top 50 ips by requests and bytes
by_ip_count = 
  FOREACH 
    (GROUP log BY FORMAT('%s', EXTRACT(remoteAddr, '(\\d+\\.\\d+\\.\\d+\\.\\d+)'))) 
  GENERATE 
    $0, 
    COUNT($1) AS num_requests, 
    SUM($1.bytes) AS num_bytes
  ;

by_ip_count_requests = 
  -- order ip by the number of requests they make
  LIMIT (ORDER by_ip_count BY num_requests DESC) 50;

STORE by_ip_count_requests into '$REPORTDIR/top_50_ips_by_requests';

by_ip_count_bytes = 
  -- order ip by the number of requests they make
  LIMIT (ORDER by_ip_count BY num_bytes DESC) 50;

STORE by_ip_count_bytes into '$REPORTDIR/top_50_ips_by_bytes';
 
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
 

-- top 50 pages by requests
by_pages_count =
  FOREACH 
    (GROUP log BY uri)
  GENERATE 
    FLATTEN($0), 
    COUNT($1) AS num_requests
  ;

by_pages_count_sorted = 
  -- take the top 50 results
  LIMIT (ORDER by_pages_count BY num_requests DESC) 50;

STORE by_pages_count_sorted INTO '$REPORTDIR/top_50_pages_by_requests';

-- top 50 pages by bytes
by_pages_bytes =
  FOREACH 
    (GROUP log BY uri)
  GENERATE 
    FLATTEN($0), 
    SUM($1.bytes) AS num_bytes
  ;

by_pages_bytes_sorted = 
  -- take the top 50 results
  LIMIT (ORDER by_pages_bytes BY num_bytes DESC) 50;

STORE by_pages_bytes_sorted INTO '$REPORTDIR/top_50_pages_by_bytes';

-- top 50 pages by average time taken
by_pages_time =
  FOREACH 
    (GROUP log BY uri)
  GENERATE 
    FLATTEN($0), 
    SUM($1.timeTaken)/COUNT($1) AS avgTime
  ;

by_pages_time_sorted = 
  -- take the top 50 results
  LIMIT (ORDER by_pages_time BY avgTime DESC) 50;

STORE by_pages_time_sorted INTO '$REPORTDIR/top_50_pages_by_timetaken';
