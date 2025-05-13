result = false;
childId = #params.childId#;
curDate = #params.servServDateAmount#;
regDate = #params.petitionReg#;
petitionId = #params.petitionId#;

calendar = null;
calendarBithday = null;
endMonthCalendar = null;
if (petitionId != null) {
  mspHolderId = #FUNC(getMSPHolderId, petitionId)#;

  if (curDate == null) {
    curDate = regDate;
  }
  if (mspHolderId != null && curDate != null) {
	relationCode = #FUNC(getRelationshipCode, childId, mspHolderId)#;
    birthday = #FUNC(getAge, mspHolderId, curDate)#;
    birthDate = #FUNC(getBirthDate, mspHolderId)#;
    calendar = Calendar.getInstance();
    calendar.setTime(curDate);
    endMonthCalendar = Calendar.getInstance();
    endMonthCalendar.setTime(curDate);
    endMonthCalendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
    if (birthDate != null) {
      calendarBithday = Calendar.getInstance();
      calendarBithday.setTime(birthDate);
    }
    sprSchool = #FUNC(hasDocumentWithoutStatus,mspHolderId,"internalStudent", endMonthCalendar.getTime(), true)#;
    sprMSE = #FUNC(hasDocumentWithoutStatus,mspHolderId,"mseNotice", endMonthCalendar.getTime(), false)#;
    category = #FUNC(hasSocialCategoryWithoutStatus,mspHolderId,"prizforcomp", curDate)# || 
	((relationCode.equals("mother") || relationCode.equals("father")) && #FUNC(hasSocialCategoryWithoutStatus,personId,"catWarLostRod", curDate)#);
	hasInv = #FUNC(hasInvReasonWithGroupAndDocument, mspHolderId, "668304")#;
    if (birthday != null && birthday < 18 && (relationCode.equals("daughter") || relationCode.equals("son"))) {result = true;}
    if (birthday != null && birthday < 23 && sprSchool) {result = true;}  
    if (birthday != null && calendarBithday != null && birthday == 18 && (calendar.get(Calendar.MONTH) == calendarBithday.get(Calendar.MONTH))) {result = true;}
    if ((sprMSE && hasInv) || (category)) {result = true;}
  }
}