package sybil.tampa.controller;

/**
 * This type was created in VisualAge.
 */

import java.util.*;
import java.io.*;
import sybil.common.util.*;
import sybil.common.persistence.*;
import sybil.common.persistence.toplink.*;
import sybil.common.model.*;

public class NotificationManager extends Thread {

	private long notificationFrequency = 60000;
	
	private Properties propInfo;

	private boolean enabled = false;

	Hashtable notify = new Hashtable();

	private static Vector statusList = new Vector();

	
/**
 * MakeNotification constructor comment.
 */
public NotificationManager() {
	super();

	String freqString;
	int notifyFreqMinutes = 0;
	int oneMinute = 60000;
	
	if ((freqString = PropertyBroker.getProperty("NOTIFICATION_FREQUENCY")) == null) {
		LogWriter.writeLog("NotificationManager(): Notification is disabled.");
		return;
	}

	try {
		notifyFreqMinutes = Integer.parseInt(freqString);
		LogWriter.writeLog("NotificationManager(): Notification frequency is set to " + 
				notifyFreqMinutes + " minutes.");
	} catch (Exception e) {
		LogWriter.writeLog("NotificationManager(): Unable to parse NOTIFICATION_FREQUENCY");
		notifyFreqMinutes = 60;	// Default to one hour
	}
	
	notificationFrequency = notifyFreqMinutes * oneMinute;
	
	enabled = true;

	loadStatusList();


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
private Vector loadPendingNotifications() {
	
	Vector pendingNotifications = null;
	String pendingStatusID = "SENDMAIL";
	
	try {
		ConnectionWrapper conn = DbConnectionManager.getInstance().getConnection();

		DbPersistentSybilEvent processor = new DbPersistentSybilEvent(conn);
		
		pendingNotifications = processor.loadAllUsingStatusID(pendingStatusID);

		DbConnectionManager.getInstance().freeConnection(conn);
	
	} catch (Exception e) {
		LogWriter.writeLog(e);
	}
	
	return pendingNotifications;

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
 * NotificationManager.main() inifileName
 *	This method must be passed a 'SybilTampa.ini' file using a fully qualified
 *	path name.  It uses this information to properly connect to Oracle.
 *
 *	In order to use this 'main' method for testing, the SYB_EVENT_LOG table must
 *	be populated with one or more rows with the following conditions met:
 *	-	status_key is set to 'SENDMAIL' (6).
 *	-	event_id_key is associated with a row in 'SYB_EVENT_ID' that is associated
 *		with an email group (group_key) in the SYB_GROUP_TABLE.
 *	-	there is one or more rows in SYB_CONTACT_GROUP to associate rows in SYB_CONTACT
 *		with the group_id in SYB_GROUP.
 *
 *	After notifications have been sent, the status_key in SYB_EVENT_LOG will be updated
 *	to 'MAILSENT' (7) so that the event will only be processed once.
 */
public static void main(String args[]) {

	try {
		PropertyBroker.load(args[0]);
	}catch(IOException ioe) {}

	try {
		DbConnectionManager.startup();
	}
	catch (Exception e) {
		// Major error--shut down sybil
		LogWriter.writeLog (e);
		System.exit(-1);
	}	


	NotificationManager mn = new NotificationManager();
	StringBuffer text = new StringBuffer("This is a test message from Sybil");
	mn.sendEmail("caums@tcs.e-mail.com", text);


}
private void processEventLogEntry(SybilEventLog log) {
	
	SybilEventType eventType = log.getEventType();
	ContactGroup group = eventType.getContactGroup();
	Vector contactList = group.getContactList();

	String emailPagerFlag = group.getEmailPagerFlag();	// 'E' or 'P'
	
	for (int i = 0; i < contactList.size(); i++) {
		TLContact contact = (TLContact) contactList.elementAt(i);
		String emailKey = null;
		if (emailPagerFlag.equals("E"))
			emailKey = contact.getEmail();
		else
			emailKey = contact.getPagerEmail();

		if (emailKey == null) {
			LogWriter.writeLog("Error! email ID not defined for contact: " + contact.getId());
		} else {
			
			if (notify.containsKey(emailKey)) {
				StringBuffer buf = (StringBuffer) notify.get(emailKey);
				buf.append(log.getMessage() + "\n");
				notify.put(emailKey, buf);
			} else {
				StringBuffer buf = new StringBuffer (log.getMessage() + "\n");
				notify.put(emailKey, buf);
			}
		}
		
	}
	
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
 *	Once all failed and successful verifications have been logged to the SybilEventLog 
 *	(SYB_EVENT_LOG) table, the notifications are produced.  All notifications for a 
 *	particular recipient (email or pager) are combined into one message and sent.
 *	However, failed and successful notifications are not combined in the same email
 *	or page.
 */
public void run() {

	Vector eventLogList;
	
	try{
		// sleep 1 min
		Thread.sleep(10000);
	} catch( InterruptedException ie) {}

	while(true) {

		notify.clear();
		
		if ((eventLogList = loadPendingNotifications()) != null) {
			
			for (int i = 0; i < eventLogList.size(); i++) {
				SybilEventLog log = (SybilEventLog) eventLogList.elementAt(i);
				processEventLogEntry(log);	// Notify everyone concerned of this event
				updateSentStatus(log);		// Update to 'MAILSENT'
			}

			Enumeration elements = notify.keys();
			while(elements.hasMoreElements()) {

				String emailID = (String)(elements.nextElement());
				StringBuffer text = (StringBuffer)(notify.get(emailID));
				sendEmail(emailID, text);

			}
		}
		
		try {
			Thread.sleep (notificationFrequency);
		} catch(InterruptedException ie) {}

	} // while(forever)

}
/**
 * This method was created in VisualAge.
 * @param message java.lang.String
 */
private void sendEmail(String emailID, StringBuffer message) {

	if (enabled) {			// Only send notification if it is turned on

		Vector emailList = new Vector();
		emailList.addElement(emailID);

		String msgTxt = message.toString();

		EmailNotification en = new EmailNotification("SybilTampa", emailList, 
			"Sybil Notification", msgTxt);

	}
	
	return;
}
private void updateSentStatus(SybilEventLog event) {
	
	SybilStatus status = getStatus("MAILSENT");

	if (status == null) {
		Exception e = new Exception ("Critical exception.  No status entry found for 'MAILSENT'. Aborting Update.");
		LogWriter.writeLog(e);
		return;
	}

	SybilStatus stClone = null;
	SybilEventLog seClone = null;
		
	try {
		ConnectionWrapper conn = DbConnectionManager.getInstance().getConnection();

		DbPersistentSybilEvent processor = new DbPersistentSybilEvent(conn);
		
//		pendingNotifications = processor.loadAllUsingStatusID(pendingStatusID);

	// Begin a transaction so we can save record to db
		processor.beginTransaction();

		seClone = (SybilEventLog)processor.register(event);
		stClone = (SybilStatus)processor.register(status);
		seClone.setSybilStatus(stClone);

		processor.commitTransaction();

		DbConnectionManager.getInstance().freeConnection(conn);
	
	} catch (Exception e) {
		LogWriter.writeLog(e);
	}
	
	return;

}
}
