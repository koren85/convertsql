SELECT MAX (t.d)
  FROM 
  
  
  (
    SELECT
      dateadd(day,1-day(wan.A_DATE_REG),wan.A_DATE_REG) d
       
    from WM_PETITION wp
      INNER JOIN WM_APPEAL_NEW wan ON wan.OUID = wp.OUID     AND ISNULL(wan.A_STATUS,10)=10
    WHERE wp.OUID = {params.petitionId}
  
    UNION
  
  SELECT
  
    dateadd(day,1-day(WM_STUDY.STARTDATE),WM_STUDY.STARTDATE) d
       
  from WM_PETITION wp
    INNER JOIN WM_APPEAL_NEW wan ON wan.OUID = wp.OUID
     AND ISNULL(wan.A_STATUS,10)=10
    /*Справка общеобразовательной организации*/
    left JOIN SPR_LINK_APPEAL_DOC linksprObshOrg
          INNER JOIN WM_ACTDOCUMENTS wasprObshOrg ON wasprObshOrg.OUID = linksprObshOrg.TOID      AND ISNULL(wasprObshOrg.A_STATUS,10)=10   
           INNER JOIN SPR_DOC_STATUS sds1 ON sds1.A_CODE = 'active'         AND sds1.A_OUID = ISNULL(wasprObshOrg.A_DOCSTATUS, sds1.A_OUID)
          INNER JOIN PPR_DOC pd1 ON pd1.A_ID = wasprObshOrg.DOCUMENTSTYPE         AND pd1.A_CODE = 'sprObshOrg'
     ON linksprObshOrg.FROMID = wp.OUID
    LEFT JOIN WM_STUDY ON WM_STUDY.OUID = wasprObshOrg.A_STUDY    
    WHERE wp.OUID = 1 and isnull (WM_STUDY.A_STATUS, 10) = 10
  
    union  
  
    SELECT
  
      wh.A_STARTDATA d
       
    from WM_PETITION wp
      INNER JOIN WM_APPEAL_NEW wan ON wan.OUID = wp.OUID    AND ISNULL(wan.A_STATUS,10)=10
   
      /*Справка мсэ*/
    left JOIN SPR_LINK_APPEAL_DOC linksprMSE 
      INNER JOIN WM_ACTDOCUMENTS wasprMSE ON wasprMSE.OUID = linksprMSE.TOID     AND ISNULL(wasprMSE.A_STATUS,10)=10 
      INNER JOIN SPR_DOC_STATUS sds ON sds.A_CODE = 'active'          AND sds.A_OUID = ISNULL(wasprMSE.A_DOCSTATUS, sds.A_OUID)
      INNER JOIN PPR_DOC pd ON pd.A_ID = wasprMSE.DOCUMENTSTYPE       AND pd.A_CODE  IN ( 'mseNotice', 'smevFgisFriReq')
    ON linksprMSE.FROMID = wp.OUID
     LEFT JOIN WM_HEALTHPICTURE wh ON wh.A_REFERENCE=linksprMSE.TOID
  WHERE wp.OUID = {params.petitionId} and isnull (wh.A_STATUS, 10) = 10
 ) t