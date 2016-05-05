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
CREATE TEXT TABLE O1 (lang VARCHAR(255), id VARCHAR(255), week VARCHAR(255), val DECIMAL(18,4));
set table O1 source '	'

CREATE TEXT TABLE O2 (lang VARCHAR(255), id VARCHAR(255), week VARCHAR(255), val DECIMAL(18,4));
set table O2 source '/Users/sadasik/Documents/crucible/sandbox/O2.csv'

SET TABLE O1 READONLY FALSE -- to add edit the table data.

SELECT * FROM O1 
MINUS
SELECT * FROM O2

RawVolFcst-FollowUpRates
--------------------------
CREATE TEXT TABLE RawVolFcstInput (lang VARCHAR(255), ou VARCHAR(255), ags VARCHAR(255), fcstGrp VARCHAR(255), mix VARCHAR(255), staffGrp VARCHAR(255), staffGrpId VARCHAR(255), fcstGrpId VARCHAR(255), week VARCHAR(255), val VARCHAR(255));
set table RawVolFcstInput source '/Users/sadasik/Documents/crucible/sandbox/1aRawVolFcstInput.csv;ignore_first=true'

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

pandas
------
Test
----
df = pd.read_csv('/Users/sadasik/Documents/crucible/sandbox/O1.csv')
lang,id,week,val
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

output
------
df = pd.read_csv('/Users/sadasik/Documents/crucible/sandbox/out.csv')
LANG,OU,AGS,FCSTGRP,STAFFGRP,HNDLMTHD,FCSTGRPID,WEEK,VAL,VALFR,VALRVF

result = df.pivot_table(index=['LANG','OU','AGS','FCSTGRP','STAFFGRP','HNDLMTHD','FCSTGRPID'], values='VAL',columns=['WEEK'])
result.to_csv('/Users/sadasik/Documents/crucible/sandbox/outTransposed.csv')
import sys
result.to_csv(sys.stdout)
