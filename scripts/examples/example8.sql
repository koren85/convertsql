SELECT CASE WHEN docDeath.OUID IS NOT NULL THEN 1 ELSE 0 END 
FROM WM_PERSONAL_CARD person
	LEFT JOIN WM_RELATEDRELATIONSHIPS relation 
		INNER JOIN SPR_GROUP_ROLE roleRelation ON relation.A_RELATED_RELATIONSHIP = roleRelation.OUID 
			AND (roleRelation.A_COD = 'mother' OR roleRelation.A_COD = 'father')
		INNER JOIN WM_PERSONAL_CARD parent ON relation.A_ID2 = parent.OUID
			AND (parent.A_STATUS = {ACTIVESTATUS} OR parent.A_STATUS IS NULL)
		-- Есть справка о смерти военнослужищего
		INNER JOIN WM_ACTDOCUMENTS docDeathMilitary
			-- Справки
			INNER JOIN PPR_DOC pprDocDeathMilitary ON pprDocDeathMilitary.A_ID = docDeathMilitary.DOCUMENTSTYPE
				AND pprDocDeathMilitary.A_CODE = 'militaryContractDeath'
		ON parent.OUID = docDeathMilitary.PERSONOUID
			AND (docDeathMilitary.A_STATUS = {ACTIVESTATUS} OR docDeathMilitary.A_STATUS IS NULL)
			AND (docDeathMilitary.ISSUEEXTENSIONSDATE IS NULL OR docDeathMilitary.ISSUEEXTENSIONSDATE <= {params.startDate})
			AND (docDeathMilitary.COMPLETIONSACTIONDATE >= {params.startDate} OR docDeathMilitary.COMPLETIONSACTIONDATE IS NULL)
		-- Есть справка о смерти 
		INNER JOIN WM_ACTDOCUMENTS docDeath
			-- Свидетельство о смерти
			INNER JOIN PPR_DOC pprDocDeath ON pprDocDeath.A_ID = docDeath.DOCUMENTSTYPE
					AND pprDocDeath.A_CODE = 'deathSprav'
			ON parent.OUID = docDeath.PERSONOUID
				AND (docDeath.A_STATUS = {ACTIVESTATUS} OR docDeath.A_STATUS IS NULL)
				AND (docDeath.ISSUEEXTENSIONSDATE IS NULL OR docDeath.ISSUEEXTENSIONSDATE <= {params.startDate})
				AND (docDeath.COMPLETIONSACTIONDATE >= {params.startDate} OR docDeath.COMPLETIONSACTIONDATE IS NULL)
	ON relation.A_ID1 = person.OUID
		AND (relation.A_STATUS = {ACTIVESTATUS} OR relation.A_STATUS IS NULL)
WHERE person.OUID = {params.personalCardId}	
	AND (person.A_STATUS = {ACTIVESTATUS} OR person.A_STATUS IS NULL)