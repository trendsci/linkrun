SET AUTOCOMMIT = ON;
CREATE DATABASE linkrundb;

CREATE SCHEMA linkrun;

DROP TABLE linkrun.temp2_07_2019;
CREATE TABLE IF NOT EXISTS linkrun.mainstats5 (
link_id TEXT PRIMARY KEY,
link_occurance int
);



INSERT INTO linkrun.mainstats VALUES ('sergey,test,com',99);


SELECT (_1,_2) from linkrun.mainstats where _1 = 'www.dsw.com' 
OR _1 = 'www.macys.com';

SELECT (_1,_2) from linkrun.mainstats where _1 = 'www.tmall.com'; 

SELECT (_1,_2) from linkrun.mainstats where _1 = 'www.invisionpower.com';


SELECT (_1,_2) from linkrun.mainstats3 where _1 = 'www.match.com' 
OR _1 = 'match.com'
OR _1 = 'tinder.com'
OR _1 = 'www.tinder.com'
OR _1 = 'okcupid.com'
OR _1 = 'www.okcupid.com' 
OR _1 = 'bumble.com'
OR _1 = 'www.bumble.com'
;

SELECT _1,_2 from linkrun.mainstats4
--ORDER BY _2 DESC
WHERE _1 = 'www.ucsd.edu'
OR _1 = 'www.1point3acres.com'
LIMIT 100;


SELECT * from linkrun.temp2
WHERE _2 = 'www.1point3acres.com'
ORDER BY _3 DESC
LIMIT 100;

SELECT * from linkrun.temp500
WHERE _2 = 'ucsd.edu'
ORDER BY _1 ASC
LIMIT 1000;


SELECT _2, sum(_3) as sum_3 from linkrun.temp500
--WHERE _2 = 'facebook.com'
GROUP BY _2
ORDER BY sum_3 DESC
LIMIT 1000;

--EXPLAIN
SELECT * from linkrun.temp500
--WHERE _2 = 'facebook.com'
ORDER BY _3 DESC
LIMIT 1000;


EXPLAIN
SELECT * from linkrun.temp500
WHERE _2 = 'bird.com'
ORDER BY _3 DESC
LIMIT 1000;


SELECT * INTO linkrun.temp500copy FROM linkrun.temp500;

DROP TABLE linkrun.temp500copy;

CREATE INDEX idx_domain_suffix
ON linkrun.temp500copy(_3 DESC);


SELECT *, (t1._3 + t2._3) as s FROM linkrun.linkrundb_30_7_2019_first1_last1 as t1
JOIN linkrun.linkrundb_30_7_2018_first1_last1 as t2
ON (t1._2 = t2._2 AND t1._1 = t2._1)
WHERE t1._2 = 'facebook.com'
AND t1._1 = $$ru-ru$$;

