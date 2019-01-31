package sybil.tampa.controller;

/**
 * This type was created in VisualAge.
 */

 	import java.io.*;
 	import java.util.*;
 	import sybil.common.util.*;
	import sybil.common.persistence.*;
	import sybil.common.persistence.toplink.*;
	import sybil.common.event.*;
	import sybil.common.model.*;
 
public class EventDBManager extends Thread{
	private int 	verificationTimeout = 0;
	private int		cleanUpTableTimeout = 0;


/**
 * EventDBManager constructor comment.
 */
public EventDBManager() {
	super();

	String str  = PropertyBroker.getProperty("VERIFICATION_TIMEOUT", "120");
	verificationTimeout = java.lang.Integer.parseInt(str.trim());

	String str2;
	if( (str2 = PropertyBroker.getProperty( "CLEANUP_TABLE")) == null){
		LogWriter.writeLog( "EventDBManager(): CLEANUP_TABLE in not defined in the ini file.");
		System.exit(1);
	}

	int intervalInDays = java.lang.Integer.parseInt(str2.trim());
	
	// number of milliseconds per day is 86400000
//	cleanUpTableTimeout = intervalInDays*(86400000);
	
//	start();
}
/**
 * This method was created in VisualAge.
 */
public java.util.Vector checkFailedVerificationStatus() {

	java.util.Vector failed = null;

	SybilStatus stat = SybilEventManager.getStatus("SENTWAIT");

	if (stat == null) {
		LogWriter.writeLog("ERROR: Status not found for 'SENTWAIT'");
		return null;
	}
	
	try {
		// Connect to database and begin a transaction
		ConnectionWrapper conn = connectDB();

		DbPersistentDatafile pdProcessor = new DbPersistentDatafile(conn);

		failed = pdProcessor.loadAllUsingTimeoutDateStatusKey(new Date(), stat.getStatusKey());

		disconnectDB(conn);	

	} catch (Exception e) {

	}
	
	return failed;
}
/**
 * This method was created in VisualAge.
 * @return sybil.common.persistence.ConnectionWrapper
 */
private ConnectionWrapper connectDB() throws DatabaseOperationFailedException {

	ConnectionWrapper conn = DbConnectionManager.getInstance().getConnection();
	
	return conn;
}
/**
 * This method was created in VisualAge.
 * @param conn ConnectionWraper
 */
private void disconnectDB(ConnectionWrapper conn) throws DatabaseOperationFailedException {

	DbConnectionManager.getInstance().freeConnection(conn);

}
/**
 * This method was created in VisualAge.
 */
public void finish() {
}
private DatafileDescriptor loadDatafileDescriptor(String delType, String procType, String dataType) {

	DatafileDescriptor datafileDescriptor = null;
	
	try {
		ConnectionWrapper conn = connectDB();

		DbPersistentDatafileDescriptor processor = new DbPersistentDatafileDescriptor(conn);
		
		datafileDescriptor = processor.loadUsingDelTypeProcTypeDataType
				(delType, procType, dataType);

		disconnectDB(conn);
	
	} catch (Exception e) {
		LogWriter.writeLog(e);
	}
	
	return datafileDescriptor;
	
}
/**
 * This method was created in VisualAge.
 * @return sybil.common.model.SybilEventType
 * @param eventId java.lang.String
 */
private SybilEventType loadEventType(String eventId) {

	SybilEventType sybilEventType = null;
	
	try {
		ConnectionWrapper conn = connectDB();

		DbPersistentSybilEventType processor = new DbPersistentSybilEventType(conn);
		
		sybilEventType = processor.loadEventType (eventId);

		disconnectDB(conn);
	
	} catch (Exception e) {
		LogWriter.writeLog(e);
	}
	
	return sybilEventType;

}
/**
 * This method was created in VisualAge.
 * @return sybil.common.model.SybilEventType
 * @param eventId java.lang.String
 */
private PlantMagIssue loadPlantMagIssue(String plant, String mag, int issueNum) {

	PlantMagIssue plantMagIssue = null;
	
	try {
		ConnectionWrapper conn = connectDB();

		DbPersistentPlantMagIssue processor = new DbPersistentPlantMagIssue(conn);
		
		plantMagIssue = processor.loadUsingPlantIdMagCodeIssueNum (plant, mag, issueNum);

		disconnectDB(conn);
	
	} catch (Exception e) {
		LogWriter.writeLog(e);
	}
	
	return plantMagIssue;

}
/**
 * This method was created in VisualAge.
 * @return sybil.common.model.SybilEventType
 * @param eventId java.lang.String
 */
private SybilEventLog loadSybilEvent(Long eventTypeKey, Long pmiKey, Long descKey) {

	SybilEventLog sybilEventLog = null;
	
	try {
		ConnectionWrapper conn = connectDB();

		DbPersistentSybilEvent processor = new DbPersistentSybilEvent(conn);
		
		sybilEventLog = processor.loadUsingIdKeyPMIKeyDescKey (eventTypeKey, pmiKey, descKey);

		disconnectDB(conn);
	
	} catch (Exception e) {
		LogWriter.writeLog(e);
	}
	
	return sybilEventLog;

}
/**
 * This method was created in VisualAge.
 * @param args java.lang.String[]
 */
public static void main(String args[]) {

	try {
		PropertyBroker.load(args[0]);
	}catch(IOException ioe) {}

	EventDBManager edb = new EventDBManager();

	SybilBusinessEvent s11 = new SybilBusinessEvent(
		SybilBusinessEvent.E_TampaProcess, 
		"SI", 
		1599, 
		"SAR",
		"SI 1599 SAR USPS strip cust - Test string for s11");

	edb.saveEvent(s11);
	
	SybilFileEvent s5 = new SybilFileEvent(
		SybilFileEvent.E_TransmitFile, 
		"PE", 
		1111, 
		"SAR",
		"CAN",
		"merge", 
		"storbook", 
		"PE 1111 SAR CAN merge storbook - Testing file event s5",
		"pe.i2345.sar.can.merge.storbook.zip", 
		3000);

	edb.saveEvent(s5);

	SybilBusinessEvent s22 = new SybilBusinessEvent(
		SybilBusinessEvent.E_TampaProcess, 
		"SI", 
		2000, 
		"PEW",
		"SI 2000 PEW USPS office cust - Test string for s22");
	edb.saveEvent(s22);

	SybilBusinessEvent s1 = new SybilBusinessEvent(
		SybilBusinessEvent.E_TampaProcess, 
		"SI", 
		1599, 
		"SAR",
		"SI 1599 SAR USPS strip cust - Test string for s1");

	edb.saveEvent(s1);
	
	SybilBusinessEvent s2 = new SybilBusinessEvent(
		SybilBusinessEvent.E_TampaProcess, 
		"SI", 
		2000, 
		"PEW",
		"SI 2000 PEW USPS office cust - Test string for s2");
	edb.saveEvent(s2);
	
	SybilFileEvent s6 = new SybilFileEvent(
		SybilFileEvent.E_PlantReceivedFile, 
		"PE", 
		1111, 
		"SAR",
		"CAN",
		"merge", 
		"storbook", 
		"PE 1111 SAR CAN merge storbook - Testing file event s6",
		"pe.i2345.sar.can.merge.storbook.zip", 
		3000);

	edb.saveEvent(s6);
	
	SybilFileEvent s3 = new SybilFileEvent(
		SybilFileEvent.E_TransmitFile, 
		"PE", 
		2345, 
		"TOR",
		"CAN",
		"merge", 
		"storbook", 
		"PE 2345 TOR CAN merge storbook - Testing file event s3",
		"pe.i2345.tor.can.merge.storbook.zip", 
		5000);

	edb.saveEvent(s3);	

	SybilFileEvent s4 = new SybilFileEvent(
		SybilFileEvent.E_PlantReceivedFile, 
		"PE", 
		2345, 
		"TOR",
		"CAN",
		"merge", 
		"storbook", 
		"PE 2345 TOR CAN merge storbook - Testing file event s4",
		"pe.i2345.tor.can.merge.storbook.zip", 
		3000);

	edb.saveEvent(s4);

	edb.finish();	
}
/**
 * This method was created in VisualAge.
 */
public void run() {

	try{
		// sleep 2 min
		Thread.sleep(120000);
	} catch( InterruptedException ie) {}
	
	while(true) {

//		edb.cleanUpTable(System.currentTimeMillis() - cleanUpTableTimeout);

		try{
			// Run once a day
			Thread.sleep(86400000);
		} catch( InterruptedException ie) {}
	
	}
	
}
/**
 * This method was created in VisualAge.
 * @param sbe sybil.common.util.SybilBusinessEvent
 */
public void saveEvent(SybilBusinessEvent sbe) {


	return;	
}
/**
 * This method was created in VisualAge.
 * @param sbe sybil.common.util.SybilBusinessEvent
 */
public void saveEvent(SybilErrorEvent see) {

	return;	
}
/**
 *	All 'SybilFileEvents' are saved here.  However, if behaves differently
 *	depending on the type of event being saved.  'saveEvent()' always inserts/
 *	updates the 'SYB_EVENT_LOG' database table.  The 'status_key' column on the
 *	SYB_EVENT_LOG table is used to set email status.  If an event requires email,
 *	the status is set to 'SENDMAIL', otherwise it is left blank (null).  When
 *	emails are sent (via NotificationManager), this status is changed to 'MAILSENT'
 *	which will prevent subsequent emails.  Present functionality handles
 *	the following situations:
 *	- E_TransmitFile: When a file is sent to the plant (by SchedulePlantDataManager)
 *		this event is generated.  It then calls 'saveVerification()', which updates
 *		the 'Datafile' table with a status of 'SENTWAIT' and sets the timeout
 *		date from the timeout interval in 'SYB_VERIFICATION'. 
 *	- E_FailedNotification: A thread in SybilEventManager periodically checks for
 *		transmissions that have timed out without acknowledgement.  It then
 *		generates this event, which causes a new event in SYB_EVENT_LOG, and
 *		probably sets notification to be emailed (i.e. status = 'SENDMAIL').  It
 *		will also call 'saveVerification()' to update Datafile with a verification
 *		status of 'SENDFAIL'.
 *	- E_PlantReceivedFile: When acknowledgements are received from the plants
 *		(via ReceiveEventManager), this event is generated.  It will generate a
 *		new event in SYB_EVENT_LOG, and may or may not generate an email.  It will
 *		also call 'saveVerification()' to update Datafile with a verification
 *		status of 'SENDGOOD'.
 */
public void saveEvent(SybilFileEvent sfe) {

	String magCode = sfe.getMag();
	int issueNum = sfe.getIssue();
	String plantId = sfe.getPlant();
	String delTypeCode = sfe.getLabelType();
	String procTypeCode = sfe.getProcType();
	String dataTypeCode = sfe.getFileType();
	String eventID = sfe.getEventID();
	String message = sfe.getMessage();
	String fileName = sfe.getFileName();
	long fileSize = sfe.getFileLength();

	SybilEventType set = null;
	SybilEventType setClone = null;

	DatafileDescriptor des = null;
	DatafileDescriptor desClone = null;

	PlantMagIssue pmi = null;
	PlantMagIssue pmiClone = null;

	SybilEventLog sybilEventLog = null;
	SybilEventLog seClone = null;

	SybilStatus status = null;
	SybilStatus stClone = null;

	try {

		ConnectionWrapper conn = connectDB();
	
	// Load Event Type
		DbPersistentSybilEventType setProcessor = new DbPersistentSybilEventType(conn);
		set = setProcessor.loadEventType (eventID);

	// Load Datafile Descriptor
		DbPersistentDatafileDescriptor ddProcessor = new DbPersistentDatafileDescriptor(conn);
		des = ddProcessor.loadUsingDelTypeProcTypeDataType (delTypeCode, procTypeCode, dataTypeCode);

	// Load PlantMagIssue
		DbPersistentPlantMagIssue pmiProcessor = new DbPersistentPlantMagIssue(conn);
		pmi = pmiProcessor.loadUsingPlantIdMagCodeIssueNum (plantId, magCode, issueNum);

	// Load SybilEvent (if it exists)
		DbPersistentSybilEvent seProcessor = new DbPersistentSybilEvent(conn);
		sybilEventLog = seProcessor.loadUsingEventIDPlantIdMagCodeIssueNumDelTypeProcTypeDataType
			(eventID, plantId, magCode, issueNum, delTypeCode, procTypeCode, dataTypeCode);
//		sybilEventLog = seProcessor.loadUsingEventIdPMIDesc (set, pmi, des);

	// Begin a transaction so we can save record to db
		seProcessor.beginTransaction();

	// Create new instance of sybilEventLog, if necessary
		if (sybilEventLog == null) {
			sybilEventLog = seProcessor.create();
		}

	// Event Type must exist
		if (set == null) {
			throw new NullPointerException("Event Type record not in database for " + eventID);
		}

	// plantMagissue should exist
		if (pmi == null) {
			throw new NullPointerException("Cannot find plant/mag/issue record in database for \r\n" +
					" Mag Code=" + magCode + ", Issue Num=" + issueNum + ", plant=" + plantId);
		}
	
	// Create a new SybilEventLog using the data needed to build the key info	
		if (des == null) {
			des = new DatafileDescriptor();
			des.setDescriptorKey(null);
		}

		seClone = (SybilEventLog)seProcessor.register(sybilEventLog);
		setClone = (SybilEventType)seProcessor.register(set);
		desClone = (DatafileDescriptor)seProcessor.register(des);
		pmiClone = (PlantMagIssue)seProcessor.register(pmi);

	// Associate foreign key relationships.
		seClone.setDescriptor(desClone);
		seClone.setEventType(setClone);
		seClone.setPlantMagIssue(pmiClone);

	// Add in any fields that will be created or updated.

		seClone.setRecordDate(new java.util.Date());
		seClone.setMessage(message);
		seClone.setFileName(fileName);
		seClone.setFileSize(fileSize);

		if (!(set.getContactGroup() == null)) {

			status = SybilEventManager.getStatus("SENDMAIL");
			stClone = (SybilStatus)seProcessor.register(status);
			seClone.setSybilStatus(stClone);
		
		}

		seProcessor.commitTransaction();

	 	seProcessor.refreshObject(sybilEventLog);

		disconnectDB(conn);

		saveVerification(sfe, set, pmi, des);

	} catch (Exception e) {
		LogWriter.writeLog(e);
	}

	return;	
}
/**
 *	Verification of datafile acknowledgement is based on files that are still
 *	in 'SENTWAIT' status and their timeout date has passed.  When this happens,
 *	this method is called and passed the offending datafile.  A new
 *	SybilFileEvent is then created and passed to the 'saveEvent()' method.
 */
public void saveFailedVerification(Datafile d) {


	Magazine mag = new Magazine(d.getFileName());
	SybilFileEvent sfe = new SybilFileEvent("E_FailedNotification", mag,
		d, "Acknowledgement not received for file <" + d.getFileName() + ">.");

	saveEvent(sfe);	

}
/**
 *	This method always updates the Datafile (SYB_DATAFILE) object/table.
 *	If datafile is null (shouldn't happen), it creates a new one using
 *	parameter data.  In any case, it sets the verification status and
 *	timeout date as appropriate based on the event ID of the SybilFileEvent.
 */
private void saveVerification(SybilFileEvent sfe, SybilEventType set,
			PlantMagIssue pmi, DatafileDescriptor des) {
	
	Datafile datafile = null;
	Datafile dfClone = null;

	SybilStatus sentwait = SybilEventManager.getStatus("SENTWAIT");
	SybilStatus sentgood = SybilEventManager.getStatus("SENTGOOD");
	SybilStatus sentfail = SybilEventManager.getStatus("SENTFAIL");
	if ((sentwait == null) || (sentgood == null) || (sentfail == null)) {
		Exception e = new Exception ("Critical exception.  No status entry found for 'SENTWAIT', 'SENTGOOD' or 'SENTFAIL'");
		LogWriter.writeLog(e);
	}

	SybilStatus theStatus = null;
	String theEventID = set.getEventID();
	if (theEventID.equals("E_PlantReceivedFile"))
		theStatus = sentgood;
	else if (theEventID.equals("E_TransmitFile"))
		theStatus = sentwait;
	else if (theEventID.equals("E_FailedNotification"))
		theStatus = sentfail;

	if (theStatus == null) return;
		
	try {
		ConnectionWrapper conn = connectDB();

		DbPersistentDatafile dfProcessor = new DbPersistentDatafile(conn);


//	* modify so that it will create new datafile if not already on db.
		datafile = dfProcessor.loadUsingDescriptorKeyPlantMagIssueKey
					(des.getDescriptorKey(), pmi.getPlantMagIssueKey());

	// Begin a transaction so we can save record to db
		dfProcessor.beginTransaction();

		if (datafile == null) {
		// Begin a transaction so we can save record to db
			datafile = dfProcessor.create();
			dfClone = (Datafile)dfProcessor.register(datafile);

			dfClone.setDescriptorKey(des.getDescriptorKey());
			dfClone.setPlantMagIssueKey(pmi.getPlantMagIssueKey());
			dfClone.setProcPlantKey(pmi.getPlantKey());
	// Need to add file date and verification date to sfe
			dfClone.setFileDate(new Date());
			dfClone.setFileName(sfe.getFileName());
			dfClone.setFileSize(sfe.getFileLength());
		} else {
			dfClone = (Datafile)dfProcessor.register(datafile);
		}

		if (theEventID.equals("E_PlantReceivedFile")) {

			dfClone.setPltRecvDate(sfe.getTimestamp());

		} else if (theEventID.equals("E_TransmitFile")) {
		
			SybilVerification sybilVerification = set.getVerification();
			Long verificationKey = sybilVerification.getVerificationKey();

			Calendar expirationDate = new GregorianCalendar();
			expirationDate.setTime(new Date());
			expirationDate.add(Calendar.MINUTE, verificationTimeout);

			dfClone.setVerificationKey(verificationKey);
			dfClone.setTpaXmitDate(new Date());
			dfClone.setTimeoutDate(expirationDate.getTime());
			dfClone.setPltRecvDate(null);

		}	// E_FailedNotification only updates 'failed' status

	// Add in any fields that apply to all cases

		dfClone.setStatusKey(theStatus.getStatusKey());

		dfProcessor.commitTransaction();
	 	dfProcessor.refreshObject(datafile);
		
		disconnectDB(conn);	

	} catch (Exception e) {
		LogWriter.writeLog(e);
	}

	return;	
}
}
