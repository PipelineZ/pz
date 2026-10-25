LOAD jev;
SET jev_api_key = 'test';
SET jev_api_url = 'http://127.0.0.1:18765/v1/systemone';
SET jev_max_retries = 1;
create table t as select * from (values (1, 'refund now or I sue'), (2, 'thanks, great service')) v(id, body);
select id, jev(t, 'the customer is angry') as angry, jev_prob(t, 'the customer is angry') as p, jev_eval(t, 'the customer is angry') as raw from t;
