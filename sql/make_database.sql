SET AUTOCOMMIT = ON;
CREATE DATABASE linkrundb;

CREATE SCHEMA linkrun;

CREATE TABLE IF NOT EXISTS linkrun.mainstats (
link_id TEXT PRIMARY KEY,
link_occurance int
);

INSERT INTO linkrun.mainstats VALUES ('sergey,test,com',99);


SELECT (_1,_2) from linkrun.mainstats where _1 = 'www.invisionpower.com';
