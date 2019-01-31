package sybil.tampa.controller;

import java.io.*;
import java.util.*;
import sybil.common.model.*;
import sybil.common.util.*;
import sybil.common.event.*;
import sybil.plant.model.*;

/**
 *		Represent the data formatting process.  It parses through a
 *		customer data file for IssueCustomers, does any formatting
 *		necessary, and passes them to the PersistentIssueCustomerManager
 *		for storing.  The persistent layer could be done in the same
 *		thread or a different thread.  See comment in the constructor.
 *
 *		@author Jun Ying
 */

public class ProcessDataForPlantThread extends Thread {

	private String fileName;
	private int numOfRecords;
	private InputFileParserFactory parserFactory;
	private InputFileParser parser;
	private PersistentIssueCustomerManager custArchiver;
	private Vector msgParms;
	private java.util.Vector listeners;
	private Thread t;
	private String plant;
	private String mag;
	private String issue;
	private String fileType;
	private String processType;
	private String labelType;
	private String dest;
	private int totalInputRecords;
	private MailStripContentParameters msParam;
	private Mag magInfo;
	private int msParamIndex;
	private String week;
	private Hashtable LORBooks;
	private int LORBookCounter = 0;
	private int LORBooksFrequency = 0;
	private boolean EndOfPKGInd = false;
	private boolean LORBookInsert = true;
	
/**
 *		Constructor.  Initializes everything.
 */
public ProcessDataForPlantThread (String fileName, String fileType, int numOfRecords, Hashtable LORBooks) {
	this.fileName = fileName;
	this.fileType = fileType;
	this.numOfRecords = numOfRecords;
	this.LORBooks = LORBooks;
	parserFactory = new InputFileParserFactory(fileType);
	listeners = new java.util.Vector();
}
/**
 * This method was created by a SmartGuide.
 * @param dal sybil.common.event.DataArrivedListener
 */
public synchronized void addIssueCustomerProcessedListener(IssueCustomerProcessedListener dal) {
		if (listeners == null) {
			listeners = new java.util.Vector();
		}
		listeners.addElement(dal);	
		return;
}
/**
 * This method was created by a SmartGuide.
 * @exception java.lang.Throwable The exception description.
 */
protected void finalize( ) throws Throwable {
//	LogWriter.writeLog("Process..Thread: <" + fileName + "> will be garbage collected!");
	return;
}
/**
 * This method was created by a SmartGuide.
 */
private synchronized void fireIssueCustomerProcessed(IssueCustomerProcessedEvent dae, String recType) {

	java.util.Vector tempListeners = null;

// The event method that is fired is dependent upon the type of data being processed.
// The parameter 'recType' is used to for this purpose.
// 	'C' = IssueCustomer Data, 'P' = Postal/Storage Book Data

	synchronized (this) {
		tempListeners = listeners;
	}
	int size = tempListeners.size();
	for (int i = 0; i < size; i++) {
		if (recType.equals("P")) {
			((IssueCustomerProcessedListener) tempListeners.elementAt(i)).storageBookProcessed(dae);
		} else {
			((IssueCustomerProcessedListener) tempListeners.elementAt(i)).issueCustomerProcessed(dae);
		}		
	}		
	return;
}
public String getFileType() {
	return fileType;
}	
public String getIssueNumber() {
	return issue;
}
public String getLabelType() {
	return labelType;
}	
public Mag getMag() {
	return magInfo;
}
public String getMagazine() {
	return mag;
}
/**
 * This method was created by a SmartGuide.
 * @return Vector msgParms
 */
public Vector getMessageParms () {
	return msgParms;
}
public String getPlant() {
	return plant;
}
public String getProcessType() {
	return processType;
}	
/**
 * This method was created in VisualAge.
 * @return 
 */
public String getWeekNumber() {
	return week;
}
private String parseDefaultDest() {
	String dest = mag.toLowerCase() + ".i" + issue.toLowerCase() + "." + 
			plant.toLowerCase() + "." + labelType.toLowerCase() + "." + processType.toLowerCase();
	if (fileType.equals("GTRF") || fileType.equals("OFCE") ||
		 fileType.equals("OMS2") || fileType.equals("GBIN")) {
		dest = dest + ".cust." + week;
	} else {
		dest = dest + "." + fileType.toLowerCase() + "." + week;
	}	
	return dest;
}	
/**
 * This method was created by a SmartGuide.
 * @return boolean
 * @param br java.io.BufferedReader
 */
private boolean processCustomerFile(DataInputStream in) {

	IssueCustomer anIssueCustomer = new IssueCustomer();
	anIssueCustomer.setalphaPlantCode(this.plant);
	
	try {

		String LORBookCheck = (String)LORBooks.get(plant);
		if (LORBookCheck != null) {
			LORBooksFrequency = Integer.valueOf(LORBookCheck).intValue();
			if (LORBooksFrequency == 0) {
				LORBookInsert = false;
			}
		}else {
			LORBookInsert = false;
		}
		
		  // parse customers using the file parser
		while (parser.parseFile(in, anIssueCustomer)) {
			totalInputRecords++;
 			LORBookCounter++;
 			EndOfPKGInd = anIssueCustomer.getEndPackageIndicator();
			IssueCustomer anIssueCustomerClone = new IssueCustomer();
			anIssueCustomerClone = anIssueCustomer;

 			
 			// determine correct destination for this customer
			dest = IssueCustomerRouteManager.determineDestination(this, anIssueCustomer);
			
			// this needs to be done inside the determineDestination method
			//anIssueCustomer.setDest(dest);
			
			// create the IssueCustomerProcessedEvent and fire it to listeners
	   		IssueCustomerProcessedEvent icpe = new IssueCustomerProcessedEvent(this, anIssueCustomer, msParam,
	   							msParamIndex);
			fireIssueCustomerProcessed(icpe, "C");
			anIssueCustomer = new IssueCustomer();
			anIssueCustomer.setalphaPlantCode(this.plant);

			//Start if for LORBooks
			if( (LORBookCounter >= LORBooksFrequency) && (EndOfPKGInd) &&(LORBookInsert)){
	 			parser.parseLORBookInsert(anIssueCustomerClone, this.plant, processType);
 				LORBookCounter = LORBookCounter - LORBooksFrequency;

 				// determine correct destination for this customer
				dest = IssueCustomerRouteManager.determineDestination(this, anIssueCustomerClone);
			
			// create the IssueCustomerProcessedEvent and fire it to listeners
		   		icpe = new IssueCustomerProcessedEvent(this, anIssueCustomerClone, msParam,
	   							msParamIndex);
				fireIssueCustomerProcessed(icpe, "C");
				anIssueCustomerClone = new IssueCustomer();
				anIssueCustomerClone.setalphaPlantCode(this.plant);

			}
			//end if for LORBooks
			
		}
	} catch (Exception e) {
		LogWriter.writeLog("Process..Thread: <" + fileName + "> Critical exception occurred on customer:");
  		LogWriter.writeLog(anIssueCustomer.toString());
		LogWriter.writeLog(e);
		return false;
	}

	return true;
}
/**
 * This method was created by a SmartGuide.
 * @return boolean
 * @param br java.io.BufferedReader
 */
private boolean processStoragePostalBookFile(BufferedReader br) {

	StoragePostalBook aPOBook = new StoragePostalBook();
	
	try {
		// parse customers using the file parser
		while (br.ready()) {
			parser.parseFile(br, aPOBook);
			totalInputRecords++;

			// determine correct destination for this customer
			dest = IssueCustomerRouteManager.determineDestination(this, aPOBook);
			
			// create the IssueCustomerProcessedEvent and fire it to listeners
   		IssueCustomerProcessedEvent icpe = new IssueCustomerProcessedEvent(this, aPOBook, 
	   					msParam, msParamIndex);
			fireIssueCustomerProcessed(icpe, "P");
			aPOBook = new StoragePostalBook();
		}
	} catch (Exception e) {
		LogWriter.writeLog("Process..Thread: <" + fileName + "> Critical exception occurred on record:");
  		LogWriter.writeLog(aPOBook.toString());
		LogWriter.writeLog(e);
		return false;
	}

	return false;
}
/**
 * This method was created by a SmartGuide.
 * @param dal sybil.common.event.DataArrivedListener
 */
public synchronized void removeIssueCustomerProcessedListener(IssueCustomerProcessedListener dal) {
		if (listeners == null) {
			return;
		}
		listeners.removeElement(dal);	
		return;
}
/**
 *		The process
 */
public void run() {
	BufferedReader br = null;
	FileInputStream fis = null;
	BufferedInputStream bis = null;
	DataInputStream dis = null;
	Runtime rt = Runtime.getRuntime();
	java.util.Hashtable h = new java.util.Hashtable();
	
	try {
		String prop = null;
		
		// Opens up the customer data file.  If the ini file defines
		// a directory for data input, prepend that directory
		if ((prop = PropertyBroker.getProperty("CustomerDataFileInputDirectory")) != null) {
			LogWriter.writeLog("Process..Thread: Processing input file <" + prop + fileName + ">.");
			fis = new FileInputStream(prop + fileName);
			bis = new BufferedInputStream(fis);
			dis = new DataInputStream(bis);
			br = new BufferedReader(new FileReader(prop + fileName));
		} else {
			LogWriter.writeLog("Process..Thread: <" + fileName + "> Data input file directory not specified in properties.  Default path used.");
			fis = new FileInputStream(fileName);
			bis = new BufferedInputStream(fis);
			dis = new DataInputStream(bis);
			br = new BufferedReader(new FileReader(fileName));
		}	
	} catch (Exception e) {
		LogWriter.writeLog(e);
	}
	
	// use the parserFactory to create the appropriate file
	// parser (GTRF, OMS2 or OFCE for customer data, STORBOOK and POBOOK for their data).
	parser = parserFactory.createInputFileParser();
	parser.setLabelType(labelType);		// Used to create correct instance of 'MagazineLabel'
	parser.setMag(magInfo);				// Used by OMS2InputFileParser
	
	if (fileType.equalsIgnoreCase("STORBOOK") || 
		fileType.equalsIgnoreCase("POBOOK") ||
		fileType.equalsIgnoreCase("STORBK2") ||
		fileType.equalsIgnoreCase("POBOOK2")) {
		processStoragePostalBookFile(br);		// Storage and Postal Book Files
	} else{
		processCustomerFile(dis);					// Customer data files.
	}		
	
	//custArchiver.setStop(true);
	// logs some relevant information	
	LogWriter.writeLog("Process..Thread: <" + fileName + "> Processing complete.");
	LogWriter.writeLog("Process..Thread: <" + fileName + "> expected records: " + numOfRecords);
	LogWriter.writeLog("Process..Thread: <" + fileName + ">   actual records: " + totalInputRecords);
	if (numOfRecords != totalInputRecords) {
		LogWriter.writeLog("Process..Thread: <" + fileName + "> ERROR!  counts do not match.");
	}	
	LogWriter.writeLog("");
	
	// uncomment this if routing needs to be changed to synchronized
	// writing to the persistence
	//custArchiver.removeThread(this);

	// IMPORTANT NOTE:  In the future, the field 'dest' could change for different customers within
	// the run.  Prior to enabling multiple routings, custArchiver.finish needs to be modified
	// to run for each 'dest' that occurred (probably by stepping through a Vector or Hashtable).
	
	if (dest == null) {
		dest = parseDefaultDest();
	}

	custArchiver.finish(dest, msgParms, msParam, msParamIndex);
	
	// remove the PersistentIssueCustomerManager from the listener vector
	// so that it will be garbage collected
	removeIssueCustomerProcessedListener(custArchiver);
	
	// stop the PersistentIssueCustomerManager if it's a separate thread
	//custArchiver = null;
}
public void setIssueNumber(String i) {
	issue = i;
}
public void setLabelType (String s) {
	labelType = s;
}	
public void setMag(Mag m) {
	magInfo = m;
}
/**
 * This method was created by a SmartGuide.
 * @param m java.lang.String
 */
public void setMagazine(String m) {
	mag = m;
}
/**
 * This method was created in VisualAge.
 */
public void setMailStripContentParam(MailStripContentParameters p,
	int index) {
	this.msParam = p;
	this.msParamIndex = index;
}
/**
 * This method was created by a SmartGuide.
 * @param msgparms Vector
 */
public void setMessageParms(Vector mp) {
	msgParms = mp;
	return;
}
/**
 * This method was created by a SmartGuide.
 * @param persisMgr sybil.tampa.controller.PersistentIssueCustomerManager
 */
public void setPersistenceManager(PersistentIssueCustomerManager persisMgr) {
	custArchiver = persisMgr;
	custArchiver.addThread(this);
	addIssueCustomerProcessedListener(custArchiver);
	return;
}
/**
 * This method was created by a SmartGuide.
 * @param p java.lang.String
 */
public void setPlant(String p) {
	plant = p;
}
public void setProcessType (String s) {
	processType = s;
}	
/**
 * This method was created in VisualAge.
 * @return 
 */
public void setWeekNumber(String w) {
	week = w;
}
}
