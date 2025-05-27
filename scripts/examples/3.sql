IF OBJECT_ID(N'EGISSO_NEW_TEMP_FACT_UPLOAD', 'U') IS NOT NULL
BEGIN
  DROP TABLE EGISSO_NEW_TEMP_FACT_UPLOAD
END;
IF OBJECT_ID(N'EGISSO_NEW_TEMP_REASON', 'U') IS NOT NULL
BEGIN
  DROP TABLE EGISSO_NEW_TEMP_REASON
END;
IF OBJECT_ID(N'EGISSO_NEW_TEMP_DOC_REASON', 'U') IS NOT NULL
BEGIN
  DROP TABLE EGISSO_NEW_TEMP_DOC_REASON
END;
IF OBJECT_ID(N'EGISSO_NEW_TEMP_DOC_ID', 'U') IS NOT NULL
BEGIN
  DROP TABLE EGISSO_NEW_TEMP_DOC_ID
END;
IF OBJECT_ID(N'EGISSO_NEW_TEMP_MSP', 'U') IS NOT NULL
BEGIN
  DROP TABLE EGISSO_NEW_TEMP_MSP
END;
IF OBJECT_ID(N'EGISSO_NEW_TEMP_INNERXML', 'U') IS NOT NULL
BEGIN
  DROP TABLE EGISSO_NEW_TEMP_INNERXML
END;
IF OBJECT_ID(N'EGISSO_NEW_TEMP_FACT_FINAL', 'U') IS NOT NULL
BEGIN
  DROP TABLE EGISSO_NEW_TEMP_FACT_FINAL
END;


CREATE TABLE EGISSO_NEW_TEMP_FACT_UPLOAD (
  A_OUID INT NOT NULL
 ,A_SERV INT NOT NULL
 ,CONSTRAINT PK_EGISSO_NEW_TEMP_FACT_UPLOAD_A_OUID PRIMARY KEY CLUSTERED (A_OUID)
) ON [PRIMARY]

CREATE TABLE EGISSO_NEW_TEMP_REASON (
  id INT NOT NULL IDENTITY
 ,ouid INT NOT NULL
 ,aSurname VARCHAR(255) NULL
 ,aName VARCHAR(255) NULL
 ,aPatronymic VARCHAR(255) NULL
 ,aSex VARCHAR(255) NULL
 ,aSnils VARCHAR(255) NULL
 ,aBirthday DATETIME NULL
 ,A_FROMID INT NOT NULL
 ,DOC_A_TYPE_EGISSO INT NULL
 ,DOC_A_NAME VARCHAR(500) NULL
 ,DOC_DOCUMENTSERIES VARCHAR(255) NULL
 ,DOC_DOCUMENTSNUMBER VARCHAR(255) NULL
 ,DOC_ISSUEEXTENSIONSDATE DATETIME NULL
 ,DOC_COMPLETIONSACTIONDATE DATETIME NULL
 ,DOC_GIVEDOCUMENTORG VARCHAR(8000) NULL
 ,CONSTRAINT PK_EGISSO_NEW_TEMP_REASON_ID PRIMARY KEY CLUSTERED (id)
) ON [PRIMARY]

CREATE TABLE EGISSO_NEW_TEMP_DOC_REASON (
  authority VARCHAR(5000) NULL
 ,finish_date DATETIME NULL
 ,issue_date DATETIME NULL
 ,number VARCHAR(255) NULL
 ,series VARCHAR(255) NULL
 ,start_date DATETIME NULL
 ,title VARCHAR(1500) NULL
 ,A_FAKT_OK INT NULL
) ON [PRIMARY]

CREATE TABLE EGISSO_NEW_TEMP_DOC_ID (
  A_LD INT NOT NULL
 ,A_TYPE_EGISSO INT NULL
 ,A_NAME VARCHAR(500) NULL
 ,DOCUMENTSERIES VARCHAR(255) NULL
 ,DOCUMENTSNUMBER VARCHAR(255) NULL
 ,ISSUEEXTENSIONSDATE DATETIME NULL
 ,COMPLETIONSACTIONDATE DATETIME NULL
 ,GIVEDOCUMENTORG VARCHAR(8000) NULL
 ,CONSTRAINT PK_EGISSO_NEW_TEMP_DOC_ID_A_LD PRIMARY KEY CLUSTERED (A_LD)
) ON [PRIMARY]

CREATE TABLE EGISSO_NEW_TEMP_MSP (
  fromId INT NOT NULL
 ,toId INT NOT NULL
 ,id INT NOT NULL
 ,CONSTRAINT PK_EGISSO_NEW_TEMP_MSP_ID PRIMARY KEY CLUSTERED (id)
) ON [PRIMARY]

CREATE TABLE EGISSO_NEW_TEMP_INNERXML (
  A_OUID INT NOT NULL
 ,DOC_RASON TEXT NULL
 ,PERSON_RASON TEXT NULL
 ,CONSTRAINT PK_EGISSO_NEW_TEMP_INNERXML_A_OUID PRIMARY KEY CLUSTERED (A_OUID)
) ON [PRIMARY]

CREATE TABLE EGISSO_NEW_TEMP_FACT_FINAL (
  DOC_A_TYPE_EGISSO INT NULL
 ,DOC_A_NAME VARCHAR(500) NULL
 ,DOC_DOCUMENTSERIES VARCHAR(255) NULL
 ,DOC_DOCUMENTSNUMBER VARCHAR(255) NULL
 ,DOC_ISSUEEXTENSIONSDATE DATETIME NULL
 ,DOC_COMPLETIONSACTIONDATE DATETIME NULL
 ,DOC_GIVEDOCUMENTORG VARCHAR(8000) NULL
 ,docReason VARCHAR(MAX) NULL
 ,persReason VARCHAR(MAX) NULL
 ,guid VARCHAR(255) NULL
 ,usingSign BIT NULL
 ,form_content VARCHAR(255) NULL
 ,form_comment VARCHAR(255) NULL
 ,form_amount FLOAT NULL
 ,FORM_EQUIVALENT_AMOUNT FLOAT NULL
 ,A_QUANTITY FLOAT NULL
 ,ouid INT NULL
 ,form_measury VARCHAR(255) NULL
 ,MSZ_receiver INT NULL
 ,LMSZID VARCHAR(255) NULL
 ,A_TITLE VARCHAR(2000) NULL
 ,guid_serv VARCHAR(255) NULL
 ,form_monetization BIT NULL
 ,decision_date DATETIME NULL
 ,dateStart DATETIME NULL
 ,dateFinish DATETIME NULL
 ,criteria VARCHAR(1500) NULL
 ,categoryID VARCHAR(255) NULL
 ,OSZCode VARCHAR(255) NULL
 ,A_NAME1 VARCHAR(255) NULL
 ,A_OKEI_CODE VARCHAR(255) NULL
 ,res_Snils VARCHAR(255) NULL
 ,res_SURNAME VARCHAR(255) NULL
 ,res_NAME VARCHAR(255) NULL
 ,res_SECONDNAME VARCHAR(255) NULL
 ,res_SEX INT NULL
 ,res_BIRTHDATE DATETIME NULL
 ,A_STATUS_FACT VARCHAR(255) NULL
 ,guidPrev VARCHAR(255) NULL
 ,OSZCodePrev VARCHAR(255) NULL
 ,A_CODE_PROVISION_FORM INT NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]


/*перечень МСП*/
INSERT INTO EGISSO_NEW_TEMP_MSP (fromId, toId, id)
  SELECT DISTINCT
    delm.A_OUID fromId
   ,snmc.A_MSP toId
   ,ROW_NUMBER() OVER (ORDER BY delm.A_OUID, snmc.A_MSP) id
  FROM DE_EGISSO_LOCAL_MSZ delm
  INNER JOIN DE_EGISSO_MAPPING_RULE demr
    ON demr.A_LOKAL_MSZ = delm.A_OUID
  INNER JOIN SPR_NPD_MSP_CAT snmc
    ON demr.A_MSP_LK_NPD = snmc.A_ID
  GROUP BY delm.A_OUID
          ,snmc.A_MSP;

/*Набор необходимых фактов*/
INSERT INTO EGISSO_NEW_TEMP_FACT_UPLOAD (A_OUID, A_SERV)
  SELECT
    amount.A_OUID
   ,serv.A_OUID
  FROM EGISSO_NEW_AMOUNT amount
  INNER JOIN EGISSO_NEW_SERV serv
    ON amount.A_SERV = serv.A_OUID
  INNER JOIN DE_EGISSO_LOCAL_MSZ delm
    ON serv.A_LMSZID = delm.A_OUID
  WHERE amount.A_STATUS = 10
  AND serv.A_STATUS = 10
  AND amount.A_UP_STATUS = 'not_load'

/*Документы идентифицирующие личность*/
INSERT INTO EGISSO_NEW_TEMP_DOC_ID
  SELECT
    query.A_LD
   ,query.A_TYPE_EGISSO
   ,query.A_NAME
   ,query.DOCUMENTSERIES
   ,query.DOCUMENTSNUMBER
   ,query.ISSUEEXTENSIONSDATE
   ,query.COMPLETIONSACTIONDATE
   ,query.GIVEDOCUMENTORG
  FROM (SELECT DISTINCT
      doc.A_LD
     ,edt.A_TYPE_EGISSO
     ,LEFT(docType.A_NAME, 100) A_NAME
     ,doc.DOCUMENTSERIES
     ,doc.DOCUMENTSNUMBER
     ,doc.ISSUEEXTENSIONSDATE
     ,doc.COMPLETIONSACTIONDATE
     ,LEFT(REPLACE(doc.GIVEDOCUMENTORG, '\\\\\\\"', ''''''), 200) GIVEDOCUMENTORG
     ,ROW_NUMBER() OVER (PARTITION BY doc.A_LD ORDER BY edt.A_NUM ASC,
      CASE
        WHEN (doc.ISSUEEXTENSIONSDATE IS NULL OR
          DATEDIFF(MONTH, GETDATE(), doc.ISSUEEXTENSIONSDATE) <= 0) AND
          (doc.COMPLETIONSACTIONDATE IS NULL OR
          DATEDIFF(MONTH, GETDATE(), doc.COMPLETIONSACTIONDATE) >= 0) THEN 0
        ELSE 1
      END, doc.ISSUEEXTENSIONSDATE DESC) AS num
    FROM IDEN_DOC_REF_REGISTRY doc
    INNER JOIN PPR_DOC docType
      ON doc.DOCUMENTSTYPE = docType.A_ID
    INNER JOIN EGISO_DOC_TYPE edt
      ON edt.A_PPR_DOC = docType.A_ID
    INNER JOIN EGISSO_NEW_LINK_SERV_PERSON link
      ON link.A_TOID = doc.A_LD
    INNER JOIN EGISSO_NEW_TEMP_FACT_UPLOAD fact
      ON fact.A_SERV = link.A_FROMID
    WHERE docType.A_ISIDENTITYCARD = 1
    AND ISNULL(doc.A_DOCSTATUS, 1) = 1
    AND ISNULL(doc.A_STATUS, 10) = 10
    AND NOT EXISTS (SELECT
        1
      FROM DE_EGISSO_REGISTER_CONFIG rc
      INNER JOIN SX_CONFIG sc
        ON rc.ouid = sc.ouid
        AND sc.A_ACTIVE = 1
      INNER JOIN IGNORE_DOC igdoc
        ON igdoc.A_FROMID = sc.ouid
      INNER JOIN PPR_DOC pd
        ON pd.A_ID = igdoc.A_TOID
      WHERE pd.A_ID = doc.DOCUMENTSTYPE
      AND (ISNULL(igdoc.A_ISNOTUPLOAD, 0) = 1
      OR (ISNULL(igdoc.A_ISNOTEMPTYUPLOAD, 0) = 1
      AND (doc.ISSUEEXTENSIONSDATE IS NULL
      AND doc.GIVEDOCUMENTORG IS NULL))))) AS query
  WHERE query.num = 1;

INSERT INTO EGISSO_NEW_TEMP_DOC_ID
  SELECT
    query.A_LD
   ,query.A_TYPE_EGISSO
   ,query.A_NAME
   ,query.DOCUMENTSERIES
   ,query.DOCUMENTSNUMBER
   ,query.ISSUEEXTENSIONSDATE
   ,query.COMPLETIONSACTIONDATE
   ,query.GIVEDOCUMENTORG
  FROM (SELECT DISTINCT
      doc.A_LD
     ,edt.A_TYPE_EGISSO
     ,LEFT(docType.A_NAME, 100) A_NAME
     ,doc.DOCUMENTSERIES
     ,doc.DOCUMENTSNUMBER
     ,doc.ISSUEEXTENSIONSDATE
     ,doc.COMPLETIONSACTIONDATE
     ,LEFT(REPLACE(doc.GIVEDOCUMENTORG, '\\\\\\\"', ''''''), 200) GIVEDOCUMENTORG
     ,ROW_NUMBER() OVER (PARTITION BY doc.A_LD ORDER BY edt.A_NUM ASC,
      CASE
        WHEN (doc.ISSUEEXTENSIONSDATE IS NULL OR
          DATEDIFF(MONTH, GETDATE(), doc.ISSUEEXTENSIONSDATE) <= 0) AND
          (doc.COMPLETIONSACTIONDATE IS NULL OR
          DATEDIFF(MONTH, GETDATE(), doc.COMPLETIONSACTIONDATE) >= 0) THEN 0
        ELSE 1
      END, doc.ISSUEEXTENSIONSDATE DESC) AS num
    FROM IDEN_DOC_REF_REGISTRY doc
    INNER JOIN PPR_DOC docType
      ON doc.DOCUMENTSTYPE = docType.A_ID
    INNER JOIN EGISO_DOC_TYPE edt
      ON edt.A_PPR_DOC = docType.A_ID
    INNER JOIN EGISSO_NEW_SERV serv
    INNER JOIN EGISSO_NEW_TEMP_FACT_UPLOAD fact
      ON fact.A_SERV = serv.A_OUID
      ON serv.A_MSZ_RECEIVER = doc.A_LD
    WHERE docType.A_ISIDENTITYCARD = 1
    AND ISNULL(doc.A_DOCSTATUS, 1) = 1
    AND NOT EXISTS (SELECT
        1
      FROM EGISSO_NEW_TEMP_DOC_ID te
      WHERE te.A_LD = doc.A_LD)

    AND ISNULL(doc.A_STATUS, 10) = 10

    AND NOT EXISTS (SELECT
        1
      FROM DE_EGISSO_REGISTER_CONFIG rc
      INNER JOIN SX_CONFIG sc
        ON rc.ouid = sc.ouid
        AND sc.A_ACTIVE = 1
      INNER JOIN IGNORE_DOC igdoc
        ON igdoc.A_FROMID = sc.ouid
      INNER JOIN PPR_DOC pd
        ON pd.A_ID = igdoc.A_TOID
      WHERE pd.A_ID = doc.DOCUMENTSTYPE
      AND (ISNULL(igdoc.A_ISNOTUPLOAD, 0) = 1
      OR (ISNULL(igdoc.A_ISNOTEMPTYUPLOAD, 0) = 1
      AND (doc.ISSUEEXTENSIONSDATE IS NULL
      AND doc.GIVEDOCUMENTORG IS NULL))))) AS query
  WHERE query.num = 1;

UPDATE EGISSO_NEW_TEMP_DOC_ID
SET DOCUMENTSERIES = REPLACE(DOCUMENTSERIES, '--', '-')
WHERE A_TYPE_EGISSO = 50

UPDATE EGISSO_NEW_TEMP_DOC_ID
SET DOCUMENTSERIES = REVERSE(STUFF(REVERSE(DOCUMENTSERIES), 3, 0, '-'))
WHERE A_TYPE_EGISSO = 50
AND DOCUMENTSERIES IS NOT NULL
AND LEN(DOCUMENTSERIES) > 2
AND CHARINDEX('-', DOCUMENTSERIES) = 0;

/*Документы основания*/
INSERT INTO EGISSO_NEW_TEMP_DOC_REASON (authority, finish_date, issue_date, number, series, start_date, title, A_FAKT_OK)
  SELECT DISTINCT
    doc.A_AUTHORITY authority
   ,doc.A_FINISH_DATE finish_date
   ,doc.A_ISSUE_DATE issue_date
   ,doc.A_NUMBER number
   ,doc.A_SERIES series
   ,doc.A_START_DATE start_date
   ,doc.A_TITLE title
   ,fact.A_SERV
  FROM EGISSO_NEW_TEMP_FACT_UPLOAD fact
  INNER JOIN EGISSO_NEW_DOC_REASON doc
    ON fact.A_SERV = doc.A_SERV
  WHERE NOT EXISTS (SELECT
      1
    FROM DE_EGISSO_REGISTER_CONFIG rc
    INNER JOIN SX_CONFIG sc
      ON rc.ouid = sc.ouid
      AND sc.A_ACTIVE = 1
    INNER JOIN IGNORE_DOC igdoc
      ON igdoc.A_FROMID = sc.ouid
    INNER JOIN PPR_DOC pd
      ON pd.A_ID = igdoc.A_TOID
    WHERE pd.GUID = doc.A_GUID_PPR
    AND (ISNULL(igdoc.A_ISNOTUPLOAD, 0) = 1
    OR (ISNULL(igdoc.A_ISNOTEMPTYUPLOAD, 0) = 1
    AND (doc.A_ISSUE_DATE IS NULL
    AND doc.A_START_DATE IS NULL
    AND doc.A_AUTHORITY IS NULL))))
  AND ISNULL(doc.A_STATUS, 10) = 10

/*Лица на основании*/
INSERT INTO EGISSO_NEW_TEMP_REASON (ouid, aSurname, aName, aPatronymic, aSex, aSnils, aBirthday, A_FROMID, DOC_A_TYPE_EGISSO, DOC_A_NAME, DOC_DOCUMENTSERIES, DOC_DOCUMENTSNUMBER, DOC_ISSUEEXTENSIONSDATE, DOC_COMPLETIONSACTIONDATE, DOC_GIVEDOCUMENTORG)
  SELECT DISTINCT
    rpc.A_OUID ouid
   ,surName.A_NAME aSurname
   ,firstName.A_NAME aName
   ,CASE
      WHEN secondName.A_NAME != '' THEN secondName.A_NAME
      ELSE NULL
    END aPatronymic
   ,rpc.A_SEX aSex
   ,CASE
      WHEN REPLACE(REPLACE(rpc.A_SNILS, '-', ''), ' ', '') != '00000000000' THEN rpc.A_SNILS
      ELSE NULL
    END aSnils
   ,rpc.BIRTHDATE aBirthday
   ,link.A_FROMID
   ,ID_DOC.A_TYPE_EGISSO DOC_A_TYPE_EGISSO
   ,ID_DOC.A_NAME DOC_A_NAME
   ,ID_DOC.DOCUMENTSERIES DOC_DOCUMENTSERIES
   ,ID_DOC.DOCUMENTSNUMBER DOC_DOCUMENTSNUMBER
   ,ID_DOC.ISSUEEXTENSIONSDATE DOC_ISSUEEXTENSIONSDATE
   ,ID_DOC.COMPLETIONSACTIONDATE DOC_COMPLETIONSACTIONDATE
   ,ID_DOC.GIVEDOCUMENTORG DOC_GIVEDOCUMENTORG
  FROM EGISSO_NEW_TEMP_FACT_UPLOAD fact
  INNER JOIN EGISSO_NEW_LINK_SERV_PERSON link
    ON fact.A_SERV = link.A_FROMID
  INNER JOIN REGISTER_PERSONAL_CARD rpc
    ON rpc.A_OUID = link.A_TOID
  LEFT JOIN EGISSO_NEW_TEMP_DOC_ID ID_DOC
    ON ID_DOC.A_LD = rpc.A_OUID
  LEFT JOIN SPR_FIO_SURNAME surName
    ON surName.ouid = rpc.SURNAME
  LEFT JOIN SPR_FIO_NAME firstName
    ON firstName.OUID = rpc.A_NAME
  LEFT JOIN SPR_FIO_SECONDNAME secondName
    ON secondName.OUID = rpc.A_SECONDNAME;

/*Сбор предподготовленных кусков xml*/
;
WITH XMLNAMESPACES ('urn://egisso-ru/types/prsn-info/1.0.4' AS prsn,
'urn://egisso-ru/types/prsn-basis/1.0.2' AS ns5,
'urn://x-artefacts-smev-gov-ru/supplementary/commons/1.0.1' AS tns,
'urn://egisso-ru/types/basic/1.0.8' AS egisso,
'urn://egisso-ru/types/assignment-fact/1.0.8' AS ns0
)
INSERT INTO EGISSO_NEW_TEMP_INNERXML (A_OUID, DOC_RASON, PERSON_RASON)
  SELECT DISTINCT
    fact.A_SERV
   ,CONVERT(NVARCHAR(MAX), (SELECT
        dr.title 'ns0:title'
       ,dr.series 'ns0:series'
       ,dr.number 'ns0:number'
       ,CONVERT(VARCHAR(10), dr.issue_date, 120) + 'Z' 'ns0:issueDate'
       ,dr.authority 'ns0:authority'
       ,CONVERT(VARCHAR(10), dr.start_date, 120) + 'Z' 'ns0:startDate'
       ,CONVERT(VARCHAR(10), dr.finish_date, 120) + 'Z' 'ns0:finishDate'
      FROM EGISSO_NEW_TEMP_DOC_REASON dr
      WHERE dr.A_FAKT_OK = fact.A_SERV
      FOR XML PATH ('ns0:document'), ROOT ('ns0:documents'))
    )
   ,CONVERT(NVARCHAR(MAX), (SELECT
        REPLACE(REPLACE(aSnils, '-', ''), ' ', '') 'prsn:prsnInfo/prsn:SNILS'
       ,aSurname 'prsn:prsnInfo/tns:FamilyName'
       ,aName 'prsn:prsnInfo/tns:FirstName'
       ,aPatronymic 'prsn:prsnInfo/tns:Patronymic'
       ,CASE aSex
          WHEN 1 THEN 'Male'
          WHEN 2 THEN 'Female'
        END 'prsn:prsnInfo/prsn:Gender'
       ,CONVERT(VARCHAR(10), aBirthday, 120) + 'Z' 'prsn:prsnInfo/prsn:BirthDate'
       ,CASE DOC_A_TYPE_EGISSO
          WHEN 10 THEN REPLACE(REPLACE(DOC_DOCUMENTSERIES, ' ', ''), '-', '')
          ELSE NULL
        END 'prsn:prsnInfo/prsn:IdentityDoc/egisso:PassportRF/egisso:Series'
       ,CASE DOC_A_TYPE_EGISSO
          WHEN 10 THEN DOC_DOCUMENTSNUMBER
          ELSE NULL
        END 'prsn:prsnInfo/prsn:IdentityDoc/egisso:PassportRF/egisso:Number'
       ,CASE DOC_A_TYPE_EGISSO
          WHEN 10 THEN CONVERT(VARCHAR(10), DOC_ISSUEEXTENSIONSDATE, 120) + 'Z'
          ELSE NULL
        END 'prsn:prsnInfo/prsn:IdentityDoc/egisso:PassportRF/egisso:IssueDate'
       ,CASE DOC_A_TYPE_EGISSO
          WHEN 10 THEN DOC_GIVEDOCUMENTORG
          ELSE NULL
        END 'prsn:prsnInfo/prsn:IdentityDoc/egisso:PassportRF/egisso:Issuer'
       ,CASE DOC_A_TYPE_EGISSO
          WHEN 20 THEN DOC_DOCUMENTSERIES
          ELSE NULL
        END 'prsn:prsnInfo/prsn:IdentityDoc/egisso:ForeignPassport/egisso:Series'
       ,CASE DOC_A_TYPE_EGISSO
          WHEN 20 THEN DOC_DOCUMENTSNUMBER
          ELSE NULL
        END 'prsn:prsnInfo/prsn:IdentityDoc/egisso:ForeignPassport/egisso:Number'
       ,CASE DOC_A_TYPE_EGISSO
          WHEN 20 THEN CONVERT(VARCHAR(10), DOC_ISSUEEXTENSIONSDATE, 120) + 'Z'
          ELSE NULL
        END 'prsn:prsnInfo/prsn:IdentityDoc/egisso:ForeignPassport/egisso:IssueDate'
       ,CASE DOC_A_TYPE_EGISSO
          WHEN 20 THEN DOC_GIVEDOCUMENTORG

          ELSE NULL
        END 'prsn:prsnInfo/prsn:IdentityDoc/egisso:ForeignPassport/egisso:Issuer'
       ,CASE DOC_A_TYPE_EGISSO
          WHEN 30 THEN DOC_DOCUMENTSERIES
          ELSE NULL
        END 'prsn:prsnInfo/prsn:IdentityDoc/egisso:ResidencePermitRF/egisso:Series'
       ,CASE DOC_A_TYPE_EGISSO
          WHEN 30 THEN DOC_DOCUMENTSNUMBER
          ELSE NULL
        END 'prsn:prsnInfo/prsn:IdentityDoc/egisso:ResidencePermitRF/egisso:Number'
       ,CASE DOC_A_TYPE_EGISSO
          WHEN 30 THEN CONVERT(VARCHAR(10), DOC_ISSUEEXTENSIONSDATE, 120) + 'Z'
          ELSE NULL
        END 'prsn:prsnInfo/prsn:IdentityDoc/egisso:ResidencePermitRF/egisso:IssueDate'
       ,CASE DOC_A_TYPE_EGISSO
          WHEN 30 THEN DOC_GIVEDOCUMENTORG
          ELSE NULL
        END 'prsn:prsnInfo/prsn:IdentityDoc/egisso:ResidencePermitRF/egisso:Issuer'
       ,CASE DOC_A_TYPE_EGISSO
          WHEN 40 THEN DOC_DOCUMENTSERIES
          ELSE NULL
        END 'prsn:prsnInfo/prsn:IdentityDoc/egisso:MilitaryPassport/egisso:Series'
       ,CASE DOC_A_TYPE_EGISSO
          WHEN 40 THEN DOC_DOCUMENTSNUMBER
          ELSE NULL
        END 'prsn:prsnInfo/prsn:IdentityDoc/egisso:MilitaryPassport/egisso:Number'
       ,CASE DOC_A_TYPE_EGISSO
          WHEN 40 THEN CONVERT(VARCHAR(10), DOC_ISSUEEXTENSIONSDATE, 120) + 'Z'
          ELSE NULL
        END 'prsn:prsnInfo/prsn:IdentityDoc/egisso:MilitaryPassport/egisso:IssueDate'
       ,CASE DOC_A_TYPE_EGISSO
          WHEN 40 THEN DOC_GIVEDOCUMENTORG
          ELSE NULL
        END 'prsn:prsnInfo/prsn:IdentityDoc/egisso:MilitaryPassport/egisso:Issuer'
       ,CASE DOC_A_TYPE_EGISSO
          WHEN 50 THEN REVERSE(STUFF(REVERSE(REPLACE(DOC_DOCUMENTSERIES, '-', '')), 3, 0, '-'))
          ELSE NULL
        END 'prsn:prsnInfo/prsn:IdentityDoc/egisso:BirthCertificate/egisso:Series'
       ,CASE DOC_A_TYPE_EGISSO
          WHEN 50 THEN DOC_DOCUMENTSNUMBER
          ELSE NULL
        END 'prsn:prsnInfo/prsn:IdentityDoc/egisso:BirthCertificate/egisso:Number'
       ,CASE DOC_A_TYPE_EGISSO
          WHEN 50 THEN CONVERT(VARCHAR(10), DOC_ISSUEEXTENSIONSDATE, 120) + 'Z'
          ELSE NULL
        END 'prsn:prsnInfo/prsn:IdentityDoc/egisso:BirthCertificate/egisso:IssueDate'
       ,CASE DOC_A_TYPE_EGISSO
          WHEN 50 THEN DOC_GIVEDOCUMENTORG
          ELSE NULL
        END 'prsn:prsnInfo/prsn:IdentityDoc/egisso:BirthCertificate/egisso:Issuer'
       ,CASE DOC_A_TYPE_EGISSO
          WHEN 60 THEN DOC_DOCUMENTSERIES
          ELSE NULL
        END 'prsn:prsnInfo/prsn:IdentityDoc/egisso:TemporaryIdentityCardRF/egisso:Series'
       ,CASE DOC_A_TYPE_EGISSO
          WHEN 60 THEN DOC_DOCUMENTSNUMBER
          ELSE NULL
        END 'prsn:prsnInfo/prsn:IdentityDoc/egisso:TemporaryIdentityCardRF/egisso:Number'

       ,CASE DOC_A_TYPE_EGISSO
          WHEN 60 THEN CONVERT(VARCHAR(10), DOC_ISSUEEXTENSIONSDATE, 120) + 'Z'
          ELSE NULL
        END 'prsn:prsnInfo/prsn:IdentityDoc/egisso:TemporaryIdentityCardRF/egisso:IssueDate'
       ,CASE DOC_A_TYPE_EGISSO
          WHEN 60 THEN DOC_GIVEDOCUMENTORG
          ELSE NULL
        END 'prsn:prsnInfo/prsn:IdentityDoc/egisso:TemporaryIdentityCardRF/egisso:Issuer'
       ,CASE DOC_A_TYPE_EGISSO
          WHEN 70 THEN DOC_DOCUMENTSERIES
          ELSE NULL
        END 'prsn:prsnInfo/prsn:IdentityDoc/prsn:RefugeeCertificate/egisso:Series'
       ,CASE DOC_A_TYPE_EGISSO
          WHEN 70 THEN DOC_DOCUMENTSNUMBER
          ELSE NULL
        END 'prsn:prsnInfo/prsn:IdentityDoc/prsn:RefugeeCertificate/egisso:Number'
       ,CASE DOC_A_TYPE_EGISSO
          WHEN 70 THEN CONVERT(VARCHAR(10), DOC_ISSUEEXTENSIONSDATE, 120) + 'Z'
          ELSE NULL
        END 'prsn:prsnInfo/prsn:IdentityDoc/prsn:RefugeeCertificate/egisso:IssueDate'
       ,CASE DOC_A_TYPE_EGISSO
          WHEN 70 THEN DOC_GIVEDOCUMENTORG
          ELSE NULL
        END 'prsn:prsnInfo/prsn:IdentityDoc/prsn:RefugeeCertificate/egisso:Issuer'
       ,CASE DOC_A_TYPE_EGISSO
          WHEN 80 THEN DOC_DOCUMENTSERIES
          ELSE NULL
        END 'prsn:prsnInfo/prsn:IdentityDoc/prsn:OtherDocument/egisso:Series'
       ,CASE DOC_A_TYPE_EGISSO
          WHEN 80 THEN DOC_DOCUMENTSNUMBER
          ELSE NULL
        END 'prsn:prsnInfo/prsn:IdentityDoc/prsn:OtherDocument/egisso:Number'
       ,CASE DOC_A_TYPE_EGISSO
          WHEN 80 THEN CONVERT(VARCHAR(10), DOC_ISSUEEXTENSIONSDATE, 120) + 'Z'
          ELSE NULL
        END 'prsn:prsnInfo/prsn:IdentityDoc/prsn:OtherDocument/egisso:IssueDate'
       ,CASE DOC_A_TYPE_EGISSO
          WHEN 80 THEN DOC_GIVEDOCUMENTORG
          ELSE NULL
        END 'prsn:prsnInfo/prsn:IdentityDoc/prsn:OtherDocument/egisso:Issuer'
      FROM EGISSO_NEW_TEMP_REASON rpc
      WHERE rpc.A_FROMID = fact.A_SERV
      FOR XML PATH ('ns5:basisPerson'), ROOT ('ns0:reasonPersons'))
    )
  FROM EGISSO_NEW_TEMP_FACT_UPLOAD fact