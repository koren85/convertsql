SELECT top 1 
case when exists
  -- Родители
  (SELECT TOP 1 child.OUID AS child
  FROM WM_RELATEDRELATIONSHIPS wr 
  INNER JOIN SPR_GROUP_ROLE sgr 
	ON sgr.OUID = wr.A_RELATED_RELATIONSHIP 
  WHERE sgr.[GUID] in ('c68c3:10b03944ac7:-7ffc','c68c3:10b03944ac7:-7ffb')
	AND wr.A_ID1 = hold.OUID 
	AND wr.A_ID2 = child.OUID 
	AND ISNULL(wr.A_STATUS,10) = 10
	) then 1
	-- вдовы (вдовцы)
	when exists
  (SELECT TOP 1 child.OUID AS child
  FROM WM_RELATEDRELATIONSHIPS wr 
  INNER JOIN SPR_GROUP_ROLE sgr 
	ON sgr.OUID = wr.A_RELATED_RELATIONSHIP 
  WHERE sgr.[GUID] in ('c68c3:10b03944ac7:-7ff7','c68c3:10b03944ac7:-7ff6')
	AND wr.A_ID1 = hold.OUID 
	AND wr.A_ID2 = child.OUID 
	AND ISNULL(wr.A_STATUS,10) = 10
  ) then 1
  -- граждане, находившиеся на иждивении погибшего (умершего) военнослужащего
  when exists
  (SELECT TOP 1 child.OUID AS child
  FROM WM_RELATEDRELATIONSHIPS wr 
  WHERE wr.A_ISDEPENDENT= 1
	AND wr.A_ID1 = hold.OUID 
	AND wr.A_ID2 = child.OUID 
	AND ISNULL(wr.A_STATUS,10) = 10
  ) then 1
  -- несовершеннолетние дети
  when exists
  (SELECT TOP 1 child.OUID AS child
  FROM WM_RELATEDRELATIONSHIPS wr 
   INNER JOIN SPR_GROUP_ROLE sgr 
	ON sgr.OUID = wr.A_RELATED_RELATIONSHIP  
  WHERE sgr.[GUID] in ('c68c3:10b03944ac7:-7ffe','c68c3:10b03944ac7:-7ffd')
	AND wr.A_ID1 = hold.OUID 
	AND wr.A_ID2 = child.OUID 
	AND ISNULL(wr.A_STATUS,10) = 10
	AND (DATEDIFF(YEAR,hold.BIRTHDATE, getdate()))+
(SIGN(DATEDIFF(DAY,hold.BIRTHDATE, DATEADD(YEAR, YEAR(hold.BIRTHDATE)-YEAR(getdate()), getdate())))-1)/2 < 18
  ) then 1
  -- дети старше 18 лет, ставшие инвалидами до достижения ими возраста 18 лет
  when exists
  (SELECT TOP 1 child.OUID AS child
  FROM WM_RELATEDRELATIONSHIPS wr 
  INNER JOIN SPR_GROUP_ROLE sgr 
	ON sgr.OUID = wr.A_RELATED_RELATIONSHIP 
  INNER JOIN WM_ACTDOCUMENTS wa 
	ON wa.PERSONOUID = hold.OUID 
	AND ISNULL(wa.A_STATUS,10) = 10
  INNER JOIN PPR_DOC pd 
	ON pd.A_ID = wa.DOCUMENTSTYPE
	AND pd.GUID IN ('7f29d2f5-6db1-418b-b5bd-24438c6145b4','d72200:10b41d50095:-7f64')
  INNER JOIN WM_HEALTHPICTURE wh 
	ON wh.A_REFERENCE = wa.OUID 
	AND ISNULL(wh.A_STATUS,10) = 10
    AND datediff(day,wh.A_DETERMINATIONS_DATE, wan.A_DATE_REG) >= 0
    AND datediff(day,wan.A_DATE_REG,wh.A_REMOVING_DATE) >= 0
  INNER JOIN SPR_INVALIDREASON si 
	ON si.OUID = wh.A_INV_REAS
	AND si.GUID = '-6cb52636:10e13ae1ca2:-7ff4'
  WHERE sgr.[GUID] in ('c68c3:10b03944ac7:-7ffe','c68c3:10b03944ac7:-7ffd')  
	AND wr.A_ID1 = hold.OUID 
	AND wr.A_ID2 = child.OUID 	
	AND ISNULL(wr.A_STATUS,10) = 10
	AND (DATEDIFF(YEAR,hold.BIRTHDATE, getdate()))+
(SIGN(DATEDIFF(DAY,hold.BIRTHDATE, DATEADD(YEAR, YEAR(hold.BIRTHDATE)-YEAR(getdate()), getdate())))-1)/2 > 18
  ) then 1
  -- дети в возрасте до 23 лет, обучающиеся в образовательных учреждениях по очной форме, в ЛД должны иметь одну из справок: "Справка о факте очного обучения для детей до 23 лет", "Справка об очной форме обучения в ВУЗе или учреждении среднего профессионального образования" или "Справка с места учебы"
  when exists
  (SELECT TOP 1 child.OUID AS child
  FROM WM_RELATEDRELATIONSHIPS wr 
  INNER JOIN SPR_GROUP_ROLE sgr 
	ON sgr.OUID = wr.A_RELATED_RELATIONSHIP 
  INNER JOIN WM_ACTDOCUMENTS wa 
	ON wa.PERSONOUID = hold.OUID 
	AND ISNULL(wa.A_STATUS,10) = 10
  INNER JOIN PPR_DOC pd 
	ON pd.A_ID = wa.DOCUMENTSTYPE
	AND pd.GUID IN ('11a4bd4:10e5b063d62:21a4','df2d38:10e7e475059:-7f02','d72200:10b41d50095:-7f66')
  where sgr.[GUID] in ('c68c3:10b03944ac7:-7ffe','c68c3:10b03944ac7:-7ffd')  
	AND wr.A_ID1 = hold.OUID 
	AND wr.A_ID2 = child.OUID 	
	AND ISNULL(wr.A_STATUS,10) = 10
	AND (DATEDIFF(YEAR,hold.BIRTHDATE, getdate()))+
(SIGN(DATEDIFF(DAY,hold.BIRTHDATE, DATEADD(YEAR, YEAR(hold.BIRTHDATE)-YEAR(getdate()), getdate())))-1)/2 < 23
  ) then 1 ELSE 0 END
    
  FROM WM_PETITION wp
  INNER JOIN WM_APPEAL_NEW wan 
	ON wan.OUID = wp.OUID
  INNER JOIN WM_PERSONAL_CARD hold 
	ON hold.OUID = wp.A_MSPHOLDER 
	AND ISNULL(hold.A_STATUS,10) = 10
  INNER JOIN WM_PERSONAL_CARD child 
	ON child.OUID = wp.A_CHILD
	AND ISNULL(child.A_STATUS,10) = 10

  WHERE wp.OUID = {params.petitionId}