SELECT
    pfv.A_VALUE / ISNULL(doc.A_COUNT_ALL_WORK_DAY, 30) * ISNULL(doc.A_COUNT_WORK_DAY, 30)
  FROM WM_PETITION wp
  INNER JOIN WM_APPEAL_NEW wan ON wan.OUID = wp.OUID
    AND wp.OUID = {params.petitionId}
  LEFT JOIN PPR_FINANCE_UNIT pfu ON ISNULL(pfu.A_STATUS, 10) = 10
    AND pfu.A_CODE = 'chaesCompZoneWork' /* Ежемесячная денежная компенсация работающим в зоне с льготн.соц.эк. статусом */
  LEFT JOIN PPR_FINANCE_VALUE pfv ON ISNULL(pfv.A_STATUS, 10) = 10
    AND pfv.A_FINANCE_UNIT = pfu.A_ID
    AND DATEDIFF(DAY, pfv.A_BEGIN_DATE, {params.startDate}) >= 0
    AND DATEDIFF(DAY, ISNULL(pfv.A_END_DATE, {params.startDate}), {params.startDate}) <= 0
  LEFT JOIN (
    SELECT
        wp.OUID AS petId
      , wa.OUID AS docId
      , twt.A_OUID AS twtId
      , twt.A_COUNT_ALL_WORK_DAY
      , twt.A_COUNT_WORK_DAY
      , ROW_NUMBER() OVER (PARTITION BY wp.OUID ORDER BY wa.ISSUEEXTENSIONSDATE DESC) num
      FROM WM_PETITION AS wp
      INNER JOIN WM_APPEAL_NEW wan ON wp.OUID = wan.OUID
      INNER JOIN WM_ACTDOCUMENTS wa ON ISNULL(wa.A_STATUS, 10) = 10
        AND wa.PERSONOUID = wp.A_MSPHOLDER
--        AND DATEDIFF(DAY, ISNULL(wa.ISSUEEXTENSIONSDATE, wan.A_DATE_REG), wan.A_DATE_REG) >= 0
--        AND DATEDIFF(DAY, ISNULL(wa.COMPLETIONSACTIONDATE, wan.A_DATE_REG), wan.A_DATE_REG) <= 0
      INNER JOIN SPR_DOC_STATUS sds ON sds.A_CODE = 'active'
        AND sds.A_OUID = ISNULL(wa.A_DOCSTATUS, sds.A_OUID)
      INNER JOIN PPR_DOC pd ON ISNULL(pd.A_STATUS, 10) = 10
        AND wa.DOCUMENTSTYPE = pd.A_ID
        AND pd.A_CODE = 'timesheet' /* Табель учета рабочего времени */
      INNER JOIN TABEL_WORK_TIME twt ON ISNULL(twt.A_STATUS, 10) = 10
        AND twt.A_DOC = wa.OUID
        AND twt.A_YEAR*100 + twt.A_MONTH = YEAR({params.startDate})*100 + MONTH({params.startDate})
    ) doc ON doc.petId = wp.OUID
    AND doc.num = 1