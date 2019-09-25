SET AUTOCOMMIT = ON;
CREATE DATABASE linkrundb;

CREATE SCHEMA linkrun;

DROP TABLE linkrun.temp2;
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
WHERE _2 = 'facebook.com'
GROUP BY _2
ORDER BY sum_3 DESC
LIMIT 1000;


SELECT * from linkrun.temp500
WHERE _2 = 'facebook.com'
ORDER BY _3 DESC
LIMIT 1000;
