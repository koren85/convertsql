SELECT CASE 
  WHEN  dateadd(year, 1, CONVERT(date, wpc.BIRTHDATE)) > CONVERT(DATE, {params.startDate})
  THEN {PPRCONST.chaesMilkChild1}
  WHEN dateadd(year, 1, CONVERT(date, wpc.BIRTHDATE)) <= CONVERT(DATE, {params.startDate})
  AND dateadd(year, 3, CONVERT(date, wpc.BIRTHDATE)) > CONVERT(DATE, {params.startDate})
  THEN {PPRCONST.chaesMilkChild3}
  END
  FROM WM_PERSONAL_CARD wpc
    WHERE wpc.OUID = {params.mspholder}