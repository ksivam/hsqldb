SELECT lang, ou, ags, fcstGrp, StaffGrp, HndlMthd, fcstGrpId, week, SUM(val) AS val FROM dbo.1aFollowUpRatesInput where fcstGrpId ='CA-CA - NonAGS - ADV - FollowUp' GROUP BY lang, ou, ags, fcstGrp, StaffGrp, HndlMthd, fcstGrpId, week;

SELECT lang, ou, ags, fcstGrp, week, SUM(val) AS val FROM dbo.1aRawVolFcstInput GROUP BY lang, ou, ags, fcstGrp, week;

SELECT FR.lang, FR.ou, FR.ags, FR.fcstGrp, FR.fcstGrpId, FR.week, SUM(FR.val)*SUM(RVF.val) AS val FROM dbo.1aFollowUpRatesInput FR, dbo.1aRawVolFcstInput RVF
WHERE FR.lang = RVF.lang AND FR.ou = RVF.ou AND FR.ags = RVF.ags AND FR.fcstGrp = RVF.fcstGrp AND FR.week = RVF.week GROUP BY FR.lang, FR.ou, FR.ags, FR.fcstGrp, FR.StaffGrp, FR.HndlMthd, FR.fcstGrpId, FR.week;

SELECT R.lang FROM
 (SELECT lang, ou, ags, fcstGrp, StaffGrp, HndlMthd, fcstGrpId, week, SUM(val) AS val FROM dbo.1aFollowUpRatesInput GROUP BY lang, ou, ags, fcstGrp, StaffGrp, HndlMthd, fcstGrpId, week) AS R;

-- SELECT FR.lang, FR.ou, FR.ags, FR.fcstGrp, FR.fcstGrpId, FR.week, SUM(FR.val)*SUM(RVF.val) AS val
SELECT FR.lang, FR.ou, FR.ags, FR.fcstGrp, FR.staffGrp, FR.HndlMthd, FR.fcstGrpId, FR.week, FR.val * RVF.val AS val,
FR.val AS valFR, RVF.val AS valRVF
FROM
(
SELECT lang, ou, ags, fcstGrp, StaffGrp, HndlMthd, fcstGrpId, week, SUM(val) AS val FROM dbo.1aFollowUpRatesInput GROUP BY lang, ou, ags, fcstGrp, StaffGrp, HndlMthd, fcstGrpId, week
) AS FR
INNER JOIN
(
SELECT lang, ou, ags, fcstGrp, week, SUM(val) AS val FROM dbo.1aRawVolFcstInput GROUP BY lang, ou, ags, fcstGrp, week
) AS RVF
ON FR.lang = RVF.lang AND FR.ou = RVF.ou AND FR.ags = RVF.ags AND FR.fcstGrp = RVF.fcstGrp AND FR.week = RVF.week;

SELECT T1.lang, T1.id, T1.week, T1.val * T2.val AS val
FROM
(
SELECT lang, id, week, SUM(val) AS val FROM dbo.T1 GROUP BY lang, id, week
) AS T1
INNER JOIN
(
SELECT lang, week, SUM(val) AS val FROM dbo.T2 GROUP BY lang, week
) AS T2
ON T1.lang = T2.lang AND T1.week = T2.week;
