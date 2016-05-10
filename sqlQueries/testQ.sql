hsqldb queries
==============
TestTable
----------

CREATE TEXT TABLE T1 (lang VARCHAR(255), id VARCHAR(255), week VARCHAR(255), val VARCHAR(255));
set table T1 source '/Users/sadasik/Documents/crucible/sandbox/T1.csv'

CREATE TEXT TABLE T2 (lang VARCHAR(255), week VARCHAR(255), val VARCHAR(255));
set table T2 source '/Users/sadasik/Documents/crucible/sandbox/T2.csv'

SELECT T1.lang, T1.id, T1.week, T1.val * T2.val AS val
FROM
(
SELECT lang, id, week, SUM(TRUNCATE(TO_NUMBER(val), 4)) AS val FROM T1 GROUP BY lang, id, week
) AS T1
INNER JOIN
(
SELECT lang, week, SUM(TRUNCATE(TO_NUMBER(val),4)) AS val FROM T2 GROUP BY lang, week
) AS T2
ON T1.lang = T2.lang AND T1.week = T2.week;

output-files
-------------
CREATE TEXT TABLE O1 (lang VARCHAR(255), id VARCHAR(255), house VARCHAR(255), week VARCHAR(255), val DECIMAL(18,4));
set table O1 source '/Users/sadasik/Documents/crucible/sandbox/O1.csv;ignore_first=true'

CREATE TEXT TABLE O2 (lang VARCHAR(255), id VARCHAR(255), week VARCHAR(255), val DECIMAL(18,4));
set table O2 source '/Users/sadasik/Documents/crucible/sandbox/O2.csv'

SET TABLE O1 READONLY FALSE -- to add edit the table data.

SELECT * FROM O1 
MINUS
SELECT * FROM O2

RawVolFcst-FollowUpRates
--------------------------
SET IGNORECASE TRUE
CREATE TEXT TABLE RawVolFcstInput (lang VARCHAR(255), ou VARCHAR(255), ags VARCHAR(255), fcstGrp VARCHAR(255), mix VARCHAR(255), staffGrp VARCHAR(255), staffGrpId VARCHAR(255), fcstGrpId VARCHAR(255), week VARCHAR(255), val VARCHAR(255));
set table RawVolFcstInput READONLY FALSE
CREATE INDEX RawVolFcstInputIndex ON RawVolFcstInput (fcstGrpId,week)
set table RawVolFcstInput source '/Users/sadasik/Documents/crucible/sandbox/1aRawVolFcstInput.csv;ignore_first=true'
EXPLAIN PLAN FOR SELECT * FROM RawVolFcstInput WHERE FCSTGRPID = 'ddff'

CREATE TEXT TABLE FollowUpRatesInput (lang VARCHAR(255), ou VARCHAR(255), ags VARCHAR(255), fcstGrp VARCHAR(255), staffGrp VARCHAR(255), HndlMthd VARCHAR(255), fcstGrpId VARCHAR(255), house VARCHAR(255), week VARCHAR(255), val VARCHAR(255));
set table FollowUpRatesInput source '/Users/sadasik/Documents/crucible/sandbox/1aFollowUpRatesInput.csv;ignore_first=true'

SELECT FR.lang, FR.ou, FR.ags, FR.fcstGrp, FR.staffGrp, FR.HndlMthd, FR.fcstGrpId, FR.week, FR.val * RVF.val AS val, FR.val AS valFR, RVF.val AS valRVF
FROM
(
SELECT lang, ou, ags, fcstGrp, StaffGrp, HndlMthd, fcstGrpId, week, SUM(TRUNCATE(TO_NUMBER(val), 4)) AS val FROM FollowUpRatesInput GROUP BY lang, ou, ags, fcstGrp, StaffGrp, HndlMthd, fcstGrpId, week
) AS FR
INNER JOIN
(
SELECT lang, ou, ags, fcstGrp, week, SUM(TRUNCATE(TO_NUMBER(val), 4)) AS val FROM RawVolFcstInput GROUP BY lang, ou, ags, fcstGrp, week
) AS RVF
ON FR.lang = RVF.lang AND FR.ou = RVF.ou AND FR.ags = RVF.ags AND FR.fcstGrp = RVF.fcstGrp AND FR.week = RVF.week

OS Network Fcst incl Transfers
------------------------------
Network Vol Allocations & Raw Vol forecast

CREATE TEXT TABLE NetworkVolAllocationsInput (lang VARCHAR(255), ou VARCHAR(255), ags VARCHAR(255), fcstGrp VARCHAR(255), staffGrp VARCHAR(255), HndlMthd VARCHAR(255), langOu VARCHAR(255), fcstGrpId VARCHAR(255), house VARCHAR(255), week VARCHAR(255), val VARCHAR(255));

SET TABLE NetworkVolAllocationsInput READONLY FALSE 
CREATE INDEX NetworkVolAllocationsInputIndex ON NetworkVolAllocationsInput (fcstGrpId,week,house)
set table NetworkVolAllocationsInput source '/Users/sadasik/Documents/crucible/sandbox/1aNetworkVolAllocationsInput.csv;ignore_first=true'
EXPLAIN PLAN FOR SELECT * FROM NETWORKVOLALLOCATIONSINPUT WHERE FCSTGRPID = 'ddff'

INSERT INTO NetworkVolAllocationsInput SELECT T.LANG, T.OU, T.AGS, T.FCSTGRP, T.STAFFGRP, T.HNDLMTHD, T.LANGOU, T.FCSTGRPID, 'Captive', T.WEEK, 1-TRUNCATE(TO_NUMBER(T.VAL), 4) FROM NetworkVolAllocationsInput T
INSERT INTO "PUBLIC"."O1" SELECT T1.LANG, T1.ID, 'captive', T1.WEEK, 1 - T1.VAL FROM "PUBLIC"."O1" T1


OS Network Fcst incl Transfers columns: Language,OU Selling Platform, Global Selling,FcstGrp,StaffGrp,Handle Method,FcstGrp ID
.............................................
test
.....
CREATE TEXT TABLE O1 (lang VARCHAR(255), id VARCHAR(255), house VARCHAR(255), week VARCHAR(255), val DECIMAL(18,4));
set table O1 source '/Users/sadasik/Documents/crucible/sandbox/O1.csv;ignore_first=true'
CREATE TEXT TABLE O2 (lang VARCHAR(255), id VARCHAR(255), week VARCHAR(255), val DECIMAL(18,4));
set table O2 source '/Users/sadasik/Documents/crucible/sandbox/O2.csv;ignore_first=true'

SELECT O1.LANG, O1.ID, O1.HOUSE, O1.WEEK, O1.VAL * O2.VAL AS VAL FROM O2, O1  WHERE O2.ID = O1.ID AND O2.WEEK = O1.WEEK AND UPPER(O1.HOUSE) = 'OUTSOURCE'

=SUMIF(OU_Contacts!$K:$K,$H1130,OU_Contacts!L:L)*SUMIFS('Network Vol. Allocations'!K:K,'Network Vol. Allocations'!$I:$I,$H1130,'Network Vol. Allocations'!$J:$J,"Outsource")

CA-CA - NonAGS - FBA - EMAIL
row 13: 529*  0.80 = 423.200

CN-US - AGS - Feedback - EMAIL, 201651 = 21126 [21,126.000, 19,687.000]

-- SELECT T1.lang, T1.ou, T1.ags, T1.fcstGrp, T1.StaffGrp, T1.HndlMthd, T1.fcstGrpId, T1.week, T1.val * T2.val AS val FROM(SELECT lang, ou, ags, fcstGrp, StaffGrp, HndlMthd, fcstGrpId, week, TRUNCATE(TO_NUMBER(val), 4) AS val FROM NetworkVolAllocationsInput GROUP BY  fcstGrpId, week) AS T1 INNER JOIN(SELECT fcstGrpId, week, TRUNCATE(TO_NUMBER(val), 4) AS val FROM RawVolFcstInput GROUP BY fcstGrpId, week) AS T2 ON T1.fcstGrpId = T2.fcstGrpId AND T1.week = T2.week

SELECT T1.lang, T1.ou, T1.ags, T1.fcstGrp, T1.StaffGrp, T1.HndlMthd, T1.fcstGrpId, T1.week, T1.val * T2.val AS val  FROM NetworkVolAllocationsInput AS T1, RawVolFcstInput AS T2 WHERE T1.fcstGrpId = T2.fcstGrpId AND T1.week = T2.week AND UPPER(T1.house) = 'OUTSOURCE'

SELECT T1.lang, T1.ou, T1.ags, T1.fcstGrp, T1.StaffGrp, T1.HndlMthd, T1.fcstGrpId, T1.week, TRUNCATE(TO_NUMBER(T1.val) * TO_NUMBER(T2.val), 4)AS VAL FROM NetworkVolAllocationsInput AS T1 INNER JOIN RawVolFcstInput AS T2 ON T1.fcstGrpId = T2.fcstGrpId AND T1.week = T2.week AND T1.house = 'OUTSOURCE'

CREATE INDEX otext ON O1 (ID,WEEK)
ALTER TABLE O1 ADD CONSTRAINT ounique UNIQUE (ID,WEEK,house)
ALTER TABLE O1 DROP CONSTRAINT OUNIQUE

pandas
------
cmd#> python
cmd#> import pandas as pd
cmd#> import sys
cmd#> pd

Test
----
df = pd.read_csv('/Users/sadasik/Documents/crucible/sandbox/O1.csv')
df.dtypes : column datatypes
-- lang,id,week,val
df.pivot_table(index=['lang','id'], values='val',columns=['week'])
o/p
week         w1   w2
lang id             
cn   cn-cn  3.0  4.0
jp   jp-jp  5.0  6.0
us   us-us  1.0  2.0

d = {
'lang' : pd.Series(['us','us', 'cn','cn','jp','jp']),
'id' : pd.Series(['us-us','us-us', 'cn-cn','cn-cn','jp-jp','jp-jp']),
'week': pd.Series(['w1','w2', 'w1','w2','w1','w2']),
'val': pd.Series([1.0,2.0,3.0,4.0,5.,6.0])
}

df = pd.DataFrame(d)

output files
------------
df = pd.read_csv('/Users/sadasik/Documents/crucible/sandbox/1aFollowUpVolOutput.csv')
-- LANG,OU,AGS,FCSTGRP,STAFFGRP,HNDLMTHD,FCSTGRPID,WEEK,VAL
result = df.pivot_table(index=['LANG','OU','AGS','FCSTGRP','STAFFGRP','HNDLMTHD','FCSTGRPID'], values='VAL',columns=['WEEK'])
result.to_csv('/Users/sadasik/Documents/crucible/sandbox/out/1aFollowUpVol.csv')
result.to_csv(sys.stdout)

df = pd.read_csv('/Users/sadasik/Documents/crucible/sandbox/1aOSNetworkFcstInclTransfersOutput.csv')
-- LANG,OU,AGS,FCSTGRP,STAFFGRP,HNDLMTHD,FCSTGRPID,WEEK,VAL
result = df.pivot_table(index=['LANG','OU','AGS','FCSTGRP','STAFFGRP','HNDLMTHD','FCSTGRPID'], values='VAL',columns=['WEEK'])
result.to_csv('/Users/sadasik/Documents/crucible/sandbox/out/1aOSNetworkFcstInclTransfers.csv')
