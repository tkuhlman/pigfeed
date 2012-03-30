-- Simply sets up my common define and sets

-- I'm using the piggybank from s3://elasticmapreduce/libs/pig/0.9.1/piggybank-0.9.1-amzn.jar
REGISTER file:/usr/share/pig/contrib/piggybank/piggybank.jar;
DEFINE DATE_TIME org.apache.pig.piggybank.evaluation.datetime.DATE_TIME();
DEFINE EXTRACT org.apache.pig.piggybank.evaluation.string.EXTRACT();
DEFINE FORMAT org.apache.pig.piggybank.evaluation.string.FORMAT();
DEFINE FORMAT_DT org.apache.pig.piggybank.evaluation.datetime.FORMAT_DT();
DEFINE REPLACE org.apache.pig.piggybank.evaluation.string.REPLACE();


-- Tuning params to experiment with
set pig.cachedbag.memusage 0.6; --My box is dedicated to pig use a lot of memory
-- set default_parallel 10;
-- set io.sort.mb 2048;
