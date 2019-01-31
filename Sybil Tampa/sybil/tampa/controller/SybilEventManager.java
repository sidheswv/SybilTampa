package sybil.tampa.controller;

/**
 * This type was created in VisualAge.
 */

 import sybil.common.util.*;
 import sybil.common.persistence.*;
 import sybil.common.persistence.toplink.*;
 import java.util.*;
 import java.io.*;
 import sybil.common.event.*;
 import sybil.common.model.*;
 
public class SybilEventManager extends Thread {
	private static long checkVerificationTimeout = 0;
	private static EventDBManager dbMgr;
	private static NotificationManager notificationMgr;

	private static Vector statusList = new Vector();

/**
 * This method was created in VisualAge.
 */
public SybilEventManager() {

	// default is 30 mins
	
	String str  = PropertyBroker.getProperty("CHECK_VERIFICATION_STATUS_TIMEOUT",
		"120");
	checkVerificationTimeout = java.lang.Long.parseLong(str);

	loadStatusList();

	dbMgr = new EventDBManager();
	notificationMgr = new NotificationManager();
	start();
}
/**
 * This method was created in VisualAge.
 * @return sybil.common.persistence.toplink.TLStatus
 * @param id java.lang.String
 */
public static SybilStatus getStatus(String id) {

	SybilStatus stat = null;
	
	for (int i = 0; i < statusList.size(); i++) {
		stat = (SybilStatus) statusList.elementAt(i);
		if (stat.getStatusID().equals(id)) {
			return stat;
		}
	}
	return null;
}
/**
 * This method was created in VisualAge.
 */
private static void loadStatusList() {

	try {
		// Connect to database and begin a transaction
		ConnectionWrapper conn = DbConnectionManager.getInstance().getConnection();
	
		// Load routeSchedule object from database if it exists using plantId, magCode, issueNum,
		// delivery type, process type and data type.  A route schedule exists if the user has
		// specifically defined one for this file
		DbPersistentStatus psProcessor = new DbPersistentStatus(conn);

		statusList = psProcessor.loadAll ();

		DbConnectionManager.getInstance().freeConnection(conn);

	} catch (Exception e) {
		LogWriter.writeLog(e);
	}	

}
/**
 * This method was created in VisualAge.
 */
public static void logEvent(SybilBusinessEvent sbe) {

	dbMgr.saveEvent(sbe);
}
/**
 * This method was created in VisualAge.
 * @param see sybil.common.util.SybilErrorEvent
 */
public static void logEvent(SybilErrorEvent see) {
	dbMgr.saveEvent(see);
}
/**
 * This method was created in VisualAge.
 * @param sfe sybil.common.util.SybilFileEvent
 */
public static void logEvent( SybilFileEvent sfe) {
	dbMgr.saveEvent(sfe);
}
/**
 * This method was created in VisualAge.
 * @param args java.lang.String[]
 */
public static void main(String args[]) {
	try{
		PropertyBroker.load(args[0]);
	} catch( Exception e) {}

	SybilEventManager sem = new SybilEventManager();
	
	SybilBusinessEvent s11 = new SybilBusinessEvent(
		SybilBusinessEvent.E_TampaProcess, 
		"SI", 
		1599, 
		"SAR",
		"SI 1599 SAR USPS strip cust - Test string for s1");

	sem.logEvent(s11);
	
	SybilFileEvent s5 = new SybilFileEvent(
		SybilFileEvent.E_TransmitFile, 
		"PE", 
		1111, 
		"SAR",
		"CAN",
		"merge", 
		"storbook", 
		"PE 1111 SAR CAN merge storbook - Testing file event s2",
		"pe.i2345.sar.can.merge.storbook.zip", 
		3000);

	sem.logEvent(s5);

	
	SybilBusinessEvent s1 = new SybilBusinessEvent(
		SybilBusinessEvent.E_TampaProcess, 
		"SI", 
		1599, 
		"SAR",
		"SI 1599 SAR USPS strip cust - Test string for s3");

	sem.logEvent(s1);
	
	
	SybilFileEvent s6 = new SybilFileEvent(
		SybilFileEvent.E_PlantReceivedFile, 
		"PE", 
		1111, 
		"SAR",
		"CAN",
		"merge", 
		"storbook", 
		"PE 1111 SAR CAN merge storbook - Testing file event s4",
		"pe.i2345.sar.can.merge.storbook.zip", 
		3000);

	sem.logEvent(s6);

	try{
		Thread.sleep(30000);
	} catch( Exception e) {}
	
	SybilFileEvent s3 = new SybilFileEvent(
		SybilFileEvent.E_TransmitFile, 
		"PE", 
		2345, 
		"TOR",
		"CAN",
		"merge", 
		"storbook", 
		"PE 2345 TOR CAN merge storbook - Testing file event s5",
		"pe.i2345.tor.can.merge.storbook.zip", 
		5000);

	sem.logEvent(s3);	

	SybilFileEvent s4 = new SybilFileEvent(
		SybilFileEvent.E_PlantReceivedFile, 
		"PE", 
		2345, 
		"TOR",
		"CAN",
		"merge", 
		"storbook", 
		"PE 2345 TOR CAN merge storbook - Testing file event s6",
		"pe.i2345.tor.can.merge.storbook.zip", 
		3000);

	sem.logEvent(s4);


	return;
}
/**
 * run()
 *	This thread checks the Datafile database table (SYB_DATAFILE) for all
 *	file transmissions (i.e. SybilFileEvent) that have not been acknowledged before
 *	timing out.  'checkFailedVerificationStatus()' reads all rows from the table
 *	where the status is 'SENTWAIT' and the 'getTimeoutDate()' is in the past.
 *
 *	It then checks for all successful verifications that require notification.  This
 *	is done by reading the Datafile database table (SYB_DATAFILE) for all
 *	file transmissions that were acknowledged as successful, and that have not already
 *	been reported (i.e. SYB_EVENT_LOG status is set to 'SENDMAIL').
 *
 */
public void run() {

	Vector failedNotifyList;
	Vector successfulNotifyList;

	try{
		// sleep 1 min
		Thread.sleep(10000);
	} catch( InterruptedException ie) {}

	while( true) {

		boolean send = false;
			
		if( (failedNotifyList = dbMgr.checkFailedVerificationStatus()) != null){
				
			for( int i = 0; i < failedNotifyList.size(); i++) {
				Datafile df = (Datafile) failedNotifyList.elementAt(i);
				LogWriter.writeLog("Verification failed for file <" + 
						df.getFileName() + ">");
				dbMgr.saveFailedVerification(df);
			}
		}
		
		try{
			Thread.sleep(checkVerificationTimeout);
		} catch(InterruptedException ie) {}
	} // while(true)
	
}
}
