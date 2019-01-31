package sybil.tampa.controller;

import java.io.*;
import java.util.*;

import sybil.common.util.*;
import sybil.common.model.*;
import sybil.common.persistence.*;
import sybil.common.persistence.toplink.*;
import sybil.common.event.*;

/**
 * This type was generated by a SmartGuide.
 * @author John Peak
 */
//============================================================
//Change Log
//============================================================
//02/04/2002
//	Applied NW 1.2 changes to W 1.0.
//============================================================
public class SchedulePlantDataManager implements Runnable {

	private String fileName = this.getClass().getName() + "[" + toString() + "]";
	private PlantMagIssueFilenameParser parser = null;

	private String dir = null;	// Directory to watch for files to process
	private String ext = null;
	private String OMSMags = null;
	private int xmitHR = 0;		// Default transmission hour 
	private int xmitMN = 0;		// Default transmission min 
	
	private File dirFile = null;
	private Vector fileFilters = new Vector();
	private int waitInterval = 10000;

	private static final int TRANSPORT_XMIT = 1;
	private static final int TRANSPORT_CDROM = 2;
	private static final long ACTION_SCHEDULED = 1;
	private static final long ACTION_ON_HOLD = 2;
	private static final long ACTION_SEND_IMMEDIATE = 3;

	private static final String DATATYPECODE_MAILDAT = "maildat";

// The following fields are populated from the Status table by 'loadStaticData()'
	private SybilStatus STATUS_NOT_SENT = null;
	private SybilStatus STATUS_SENT = null;

// The following are for setting the last modified time
	private long lastModCurrTime = 0;
	private boolean lastModOK = false;

/**
 Default constructor
*/

public SchedulePlantDataManager() {
		
	String t = PropertyBroker.getProperty("ScheduleDefaultSendTime", "1500");
	try {
		xmitHR = (Integer.parseInt(t.substring(0,2)));
		xmitMN = (Integer.parseInt(t.substring(2)));
	} catch (NumberFormatException ex) {
		LogWriter.writeLog("SchedulePlantDataManager: Unable to parse def transmit time - " + t);
		xmitHR = 15;
		xmitMN = 0;
	}
		
	dir = PropertyBroker.getProperty("TransmissionStageOutputDirectory");

	if (dir == null) {
		SybilWarningException e = new SybilWarningException 
					("SchedulePlantDataManager: No File Input Directory specified.");
		LogWriter.writeLog(e);
		System.exit(1);
	}

	dirFile = new File(dir);
	if (!dirFile.exists() || !dirFile.isDirectory()) {
		SybilWarningException e = new SybilWarningException
				("SchedulePlantDataManager: Directory specified does not exist or is not a directory.");
		LogWriter.writeLog(e);
		System.exit(1);
	}
	
	// initialize the filter
	ext = PropertyBroker.getProperty("ScheduleFileExtention");
	if (ext == null) {
		SybilWarningException e = new SybilWarningException
				("SchedulePlantDataManager: No file extention specified.");
		LogWriter.writeLog(e);
		System.exit(1);
	} else {
		char[] tokenSep = {',', '\r'};
		StringTokenizer st = new StringTokenizer(ext, new String(tokenSep));
		int numEntries = st.countTokens();
		for (int i = 0; i < numEntries; i++) {
			fileFilters.addElement(new FileExtentionFilter(st.nextToken()));
		}
	}

	// initialize the filter
	String sleepVal = PropertyBroker.getProperty("ScheduleOutputFileSleepValue");
	if (sleepVal == null) {
		waitInterval = 60000;
	} else {
		try {
			waitInterval = Integer.parseInt(sleepVal);
		} catch (NumberFormatException ex) {
			LogWriter.writeLog(ex);
			waitInterval = 60000;
		}
	}

	loadStaticData();
		
	LogWriter.writeLog(" SchedulePlantDataManager will be watching for files with extension <" + ext + ">");
	LogWriter.writeLog(" on the system directory path <" + dir + ">.");

	String prop = null;
	if( (prop = PropertyBroker.getProperty("OMSMagazines")) == null) {
		SybilWarningException e = new SybilWarningException(
		"SchedulePlantDataManager(): OMSMagazines is not defined.");
		LogWriter.writeLog(e);
		return;
	}
	OMSMags = prop.trim().toLowerCase();

}
/**
 * This method was created in VisualAge.
 * @param f sybil.common.model.SybilFile
 */
private void handleSchedulingError(SybilFile f) {

	String dataFileName = f.getName();

	// This parser breaks down dataFileName into the data elements we need		
	Hashtable dataElements = parser.parseFilename(dataFileName);

	String plantId = (String)dataElements.get("plantId");
	String magCode = (String)dataElements.get("magCode");
	int issueNum = ((Integer)dataElements.get("issueNum")).intValue();


	
	// Get Destination 'Archive' Directory to move processed file to.
	String archiveDir = PropertyBroker.getProperty("DataFileArchiveDirectory");
	if (archiveDir == null) {
		SybilWarningException e = new SybilWarningException 
				("SchedulePlantDataManager: DataFileArchiveDirectory not found " +
					"File not moved to archive directory.");
		LogWriter.writeLog(e);
	}

	// Get Destination 'ScheduleErrors' Directory to move processed file to.
	//String errorDir = PropertyBroker.getProperty("ScheduleOutputErrorDir");
	//if (archiveDir == null) {
	//	SybilWarningException e = new SybilWarningException 
	//			("SchedulePlantDataManager: ScheduleOutputErrorDir not found " +
	//				"File not moved to ScheduleErrors directory.");
	//	LogWriter.writeLog(e);
	//}
		
	// Part 1) COPY file to the error archive 'hold' directory (errorDir).
	//File errorFile = new File(errorDir, dataFileName);
	//if (errorFile.exists()) {
	//	errorFile.delete();
	//}	
	//boolean success1 = f.copyTo(errorFile);
	//if (!success1) {
	//	SybilWarningException e = new SybilWarningException("SchedulePlantDataManager: Could not COPY file " + 
	//					f.toString() + " to " + errorFile.toString() + ".  Make sure file is manually copied.");
	//	LogWriter.writeLog(e);
	//}
		
	// Part 2) MOVE file (f) from 'ToSend' to 'archiveDir' directory (i.e. 'Sent').
	File archiveFile = new File(archiveDir, dataFileName);
	if (archiveFile.exists()) {
		archiveFile.delete();
	}
	boolean success3 = f.renameTo(archiveFile);
	if (!success3) {
		SybilWarningException e = new SybilWarningException("SchedulePlantDataManager.handleSchedulingError ... Could not MOVE file " + 
			f.toString() + " to " + archiveFile.toString() + ".  Make sure file is manually moved.");
		LogWriter.writeLog(e);
	}
	// Set last modified time of moved file so that create and modified time are the same.
	else {
		try {
			lastModCurrTime = System.currentTimeMillis();
			lastModOK = archiveFile.setLastModified(lastModCurrTime);
			if(!lastModOK) {
				LogWriter.writeLog("ERROR: SchedulePlantDataManager.handleSchedulingError ... " + 
					"Could not set last modified time on " + archiveFile.toString());
			}
		}
		catch (Exception e) {
			LogWriter.writeLog(e);
			LogWriter.writeLog("ERROR: SchedulePlantDataManager.handleSchedulingError ... " + 
				"Could not set last modified time on " + archiveFile.toString());
		}
	}

	// Part 3) Notify someone that this file could not be scheduled.

/*	SybilErrorEvent evt = new SybilErrorEvent ("SCHEDERR", magCode, issueNum, plantId,
		"Sybil unable to schedule file <" + dataFileName + ">. \r\n\t\t" +
		"File moved to <" + errorDir + ">.");
	SybilEventManager.logEvent(evt);
*/
	return;
}
/**
 * This method was created in VisualAge.
 */
private void loadStaticData() {

	Vector statusList = new Vector();
	SybilStatus stat = null;
	Long statusKey = null;
	
	STATUS_NOT_SENT = SybilEventManager.getStatus("NOTSENT");
	
	STATUS_SENT = SybilEventManager.getStatus("SENTWAIT");

	
	if ((STATUS_NOT_SENT == null) || (STATUS_SENT == null)) {
		SybilWarningException e = new SybilWarningException ("Error retrieving required status codes " +
			"on STATUS table.  Must provide entry with status_id = 'NOTSENT', and 'SENTWAIT'" +
			" Please add required entries to the SYS_STATUS table and restart Sybil");
		LogWriter.writeLog(e);
	}
	
}
/**
 * This method was created in VisualAge.
 * @param df sybil.common.model.SybilFile
 */
private void moveFile(SybilFile df) {
	
	String dataFileName = df.getName();
			
	try {
		
		// Get Destination 'Archive' Directory to move processed file to.
		String archiveDir = PropertyBroker.getProperty("DataFileArchiveDirectory");
		if (archiveDir == null) {
			SybilWarningException e = new SybilWarningException 
					("SchedulePlantDataManager: DataFileArchiveDirectory not found " +
						"File not moved to archive directory.");
			LogWriter.writeLog(e);
		}
		else {
			// MOVE file (df) from 'ToSend' to 'archiveDir' directory (i.e. 'Sent').
			File archiveFile = new File(archiveDir, dataFileName);

			if (archiveFile.exists()) {
				archiveFile.delete();
			}
		
			boolean success3 = df.renameTo(archiveFile);
			
			if (!success3) {
				SybilWarningException e = new SybilWarningException("SchedulePlantDataManager: Could not MOVE file " + 
					df.toString() + " to " + archiveFile.toString() + ".  Make sure file is manually moved.");
				LogWriter.writeLog(e);
			}
			// Set last modified time of moved file so that create and modified time are the same.
			else {
				try {
					lastModCurrTime = System.currentTimeMillis();
					lastModOK = archiveFile.setLastModified(lastModCurrTime);
					if(!lastModOK) {
						LogWriter.writeLog("ERROR: SchedulePlantDataManager.moveFile ... " + 
							"Could not set last modified time on " + archiveFile.toString());
					}
				}
				catch (Exception e) {
					LogWriter.writeLog(e);
					LogWriter.writeLog("ERROR: SchedulePlantDataManager.moveFile ... " + 
						"Could not set last modified time on " + archiveFile.toString());
				}
			}
		}
	}
	catch (Exception e) {
		LogWriter.writeLog(e);
	}

	return;
}
/**
 * This method was created in VisualAge.
 */
public void run () {

	int maxFilters = fileFilters.size();
	
	while (true) {

		for (int j = 0; j < maxFilters; j++) {
			String [] files = dirFile.list((FileExtentionFilter)fileFilters.elementAt(j));
			int numFiles = files.length;
			
			for (int i = 0; i < numFiles; i++) {
				SybilFile f = (new SybilFile(dir + files[i]));
				scheduleDataFile(f);
			}	

		}

		try {
			Thread.sleep(waitInterval);
		} catch (InterruptedException ie) {}		

	}	
	
}
/**
 * This method will routeSched all datafiles that have finished being processed to
 * be transmitted (via being moved to the WhamNet! directory) according to parms
 * setup in the database.  Note that DataArrivedChecker only notifies us of new 
 * files added to the directory.  If the file does not cofirm to the naming required
 * to lookup the sheduling parms a message is written to the event log.
 * @param dae sybil.common.event.DataArrivedEvent
 */
private void scheduleDataFile(SybilFile df) {
	// Get the name of the file being scheduled
	String dataFileName = df.getName();
	boolean success = true;
	Date schedXmitDate = null;
	Date nowDate = null;
	boolean newRec = false;
	
	// Parse dataFileName into a hashtable using parser--perform lazy init if null
	if (parser == null) {
		parser = new PlantMagIssueFilenameParser();
	}

	// This parser breaks down dataFileName into the data elements we need		
	Hashtable dataElements = parser.parseFilename(dataFileName);

	nowDate = new java.util.Date();

	String plantId = (String)dataElements.get("plantId");
	String magCode = (String)dataElements.get("magCode");
	int issueNum = ((Integer)dataElements.get("issueNum")).intValue();
	String delTypeCode = (String)dataElements.get("deliveryType");
	String procTypeCode = (String)dataElements.get("processType");
	String dataTypeCode = (String)dataElements.get("dataType");	// Not all files have datatype

	// Release 1 mag maildat files are not sent to plant here. Just move to "sent" dir.
	if((OMSMags != null) && (OMSMags.indexOf(magCode.toLowerCase()) == -1)) {
		if(dataTypeCode.toLowerCase().equals(DATATYPECODE_MAILDAT)) {
			LogWriter.writeLog("SchedulePlantDataManager: Release 1 mag maildat. Move without sending to plant.");
			LogWriter.writeLog("SchedulePlantDataManager: Mag=" + magCode + " Plant=" + plantId +
				" Del=" + delTypeCode + " Proc=" + procTypeCode + " Data=" + dataTypeCode);
			moveFile (df);
			return;
		}
	}
	
	try {
		// Connect to database and begin a transaction
		ConnectionWrapper conn = DbConnectionManager.getInstance().getConnection();
	
		// Load routeSchedule object from database if it exists using plantId, magCode, issueNum,
		// delivery type, process type and data type.  A route schedule exists if the user has
		// specifically defined one for this file
		DbPersistentRouteSchedule rsProcessor = new DbPersistentRouteSchedule(conn);
		RouteSchedule routeSchedule = null;

		routeSchedule = rsProcessor.loadUsingPlantIdMagCodeIssueNumDelTypeProcTypeDataType
				(plantId, magCode, issueNum, delTypeCode, procTypeCode, dataTypeCode);
	
		// If routeSchedule does not exist then create it from the defRouteSchedule
		if (routeSchedule == null) {
			DefRouteSchedule defRouteSchedule = null;

			// Load default route schedule
			DbPersistentDefRouteSchedule drsProcessor = new DbPersistentDefRouteSchedule(conn);	
			defRouteSchedule = drsProcessor.loadUsingPlantIdMagCodeDelTypeProcTypeDataType
						(plantId, magCode, delTypeCode, procTypeCode, dataTypeCode);

			// A default route schedule must exist otherwise error
			if (defRouteSchedule == null) {
				SybilWarningException e = new SybilWarningException("Default Route " +
						"Schedule does not exist in the database for \r\n" +
						"Plant ID=" + plantId + ", Mag Code=" + magCode + ", Delivery Type=" + delTypeCode + 
						", Process Type=" + procTypeCode + ", Data Type=" + dataTypeCode);
				LogWriter.writeLog(e);
				success = false;
			}

			// Load plantMagIssue
			DbPersistentPlantMagIssue pmiProcessor = new DbPersistentPlantMagIssue(conn);	
			PlantMagIssue plantMagIssue = pmiProcessor.loadUsingPlantIdMagCodeIssueNum
							(plantId, magCode, issueNum);
	
			// If plantMagIssue does not exist then create it	
			if (plantMagIssue == null) {
				// Load magIssue if it exists
				DbPersistentMagIssue miProcessor = new DbPersistentMagIssue(conn);	
				MagIssue magIssue = miProcessor.loadUsingMagCodeIssueNum(magCode, issueNum);
		
				// If magIssue does not exist then create it
				if (magIssue == null) {
					SybilWarningException e = new SybilWarningException("Magazine-" +
						"Issue does not exist in the database for \r\n" +
								", Mag Code=" + magCode + ", Issue Num=" + issueNum);
					LogWriter.writeLog(e);
					success = false;
				}	

				// PlantMag comes from the defRouteSchedule--It should always exist
				PlantMag plantMag = defRouteSchedule.getPlantMag();

				// If plantMag does not exist then error out
				if (plantMag == null) {
					SybilWarningException e = new SybilWarningException("Plant-" +
						"Magazine does not exist in the database for \r\n" +
								"Plant ID=" + plantId + ", Mag Code=" + magCode);
					LogWriter.writeLog(e);
					success = false;
				}
			
				// Get plant from plantMag
				Plant plant = plantMag.getPlant();
			
				// Begin a transaction so we can save record to db
				pmiProcessor.beginTransaction();

				// Create plantMagIssue from a plant and a magIssue
				plantMagIssue = pmiProcessor.create();
				plantMagIssue.setPlantKey(plant.getPlantKey());
				plantMagIssue.setMagIssueKey(magIssue.getMagIssueKey());
				plantMagIssue.setPlant(plant);
				plantMagIssue.setMagIssue(magIssue);

				PlantMagIssue pmiClone = (PlantMagIssue)pmiProcessor.register(plantMagIssue);
			
				// Save the record to the database
				pmiProcessor.commitTransactionAndResume();
			}
		
		// Create route schedule from default route schedule			
			Plant procPlant = null;
			procPlant = defRouteSchedule.getProcPlant();

		// Create route schedule from default route schedule			
			DatafileDescriptor descriptor = null;
			descriptor = defRouteSchedule.getDescriptor();

		// Begin a transaction so we can save record to db
			rsProcessor.beginTransaction();

		// Create route schedule from default route schedule			
			routeSchedule = rsProcessor.create();
			routeSchedule.setDescriptorKey(descriptor.getDescriptorKey());
			routeSchedule.setPlantMagIssueKey(plantMagIssue.getPlantMagIssueKey());
			routeSchedule.setProcPlantKey(procPlant.getPlantKey());
			routeSchedule.setProcPlant(procPlant);
			routeSchedule.setPlantMagIssue(plantMagIssue);

		// Set necessary parms			
			routeSchedule.setTransport(defRouteSchedule.getTransport());
			routeSchedule.setAction(defRouteSchedule.getAction());
	
		// Set Xmit date here
			Calendar calendar = new GregorianCalendar();
			Date coverDate = plantMagIssue.getMagIssue().getCoverDate();
			calendar.setTime(coverDate);
		// Formula for subtracting the offset start value from the cover date which 
		// gives us the transmission date
			int offsetStart = (defRouteSchedule.getOffsetStart()) * -1;
			calendar.add(Calendar.DATE, offsetStart);
			calendar.set(Calendar.HOUR, xmitHR);
			calendar.set(Calendar.MINUTE, xmitMN);
			routeSchedule.setXmitDate(calendar.getTime());
				
		// Save the record to the database
			RouteSchedule rsClone = (RouteSchedule)rsProcessor.register(routeSchedule);
			rsProcessor.commitTransaction();
		}	
		
		// Get our retrieval criteria for datafile
		Long descriptorKey = routeSchedule.getDescriptorKey();
		Long plantMagIssueKey = routeSchedule.getPlantMagIssueKey();
		Date fileDate = new Date(df.lastModified());

		// Create a datafile process so we can retrieve/create a datafile
		// record in the database
		DbPersistentDatafile dfProcessor = new DbPersistentDatafile(conn);
		Datafile dataFile = dfProcessor.loadUsingDescriptorKeyPlantMagIssueKey(descriptorKey, plantMagIssueKey);
		
		if (dataFile == null) {
		// Begin a transaction so we can save record to db
			dfProcessor.beginTransaction();
			dataFile = dfProcessor.create();
			Datafile dfClone = (Datafile)dfProcessor.register(dataFile);

			dfClone.setDescriptorKey(routeSchedule.getDescriptorKey());
			dfClone.setPlantMagIssueKey(routeSchedule.getPlantMagIssueKey());
			dfClone.setProcPlantKey(routeSchedule.getProcPlantKey());
			dfClone.setFileDate(fileDate);
			dfClone.setFileName(dataFileName);
			dfClone.setFileSize(df.length());
			dfClone.setStatusKey(STATUS_NOT_SENT.getStatusKey());	// 'NOTSENT'
			dfClone.setTpaSchedDate(new Date());
			dfClone.setVerificationKey(null);
			dfProcessor.commitTransaction();
			newRec = true;
		}

	// Force refresh of routeSchedule and dataFile in case it was updated
		rsProcessor.refreshObject(routeSchedule);
	 	dfProcessor.refreshObject(dataFile);

	// Free database connection
		DbConnectionManager.getInstance().freeConnection(conn);

		if (newRec) return;

	// Determine if the file should be sent or not.  If so, then send it.
		schedXmitDate = routeSchedule.getXmitDate();
		nowDate = new Date();

		int transport = routeSchedule.getTransport();
		int action = routeSchedule.getAction();

		if (transport == TRANSPORT_XMIT && action == ACTION_SCHEDULED) {
			if (nowDate.after(schedXmitDate)) {
				sendFile (df, dataFile, routeSchedule);
			}
		} else if (action == ACTION_SEND_IMMEDIATE) {
			sendFile (df, dataFile, routeSchedule);
		}
			
	}
	catch (DatabaseOperationFailedException e) {
		// General database failuer--lost connection
		LogWriter.writeLog(e);
		success = false;
	}
	catch (NullPointerException e) {
		// Missing initial-load record from database
		LogWriter.writeLog(e);
		success = false;
	}
	catch (Exception e) {
		LogWriter.writeLog(e);
		success = false;
	}	

	// If something failed along the way then warn user
	if (!success) {
		//SybilWarningException e = new SybilWarningException("Could not schedule file " + dataFileName + ".");
		//LogWriter.writeLog(e);
		handleSchedulingError(df);
	}
	return;
}
/**
 * This method was created by a SmartGuide.
 * @param theEvent sybil.tampa.controller.SendFileEvent
 */
private void sendFile(SybilFile df, Datafile datafile, RouteSchedule routeSchedule) {

	String dataFileName = df.getName();
	Runtime rt = Runtime.getRuntime();
	String multiMailFileName = null;
	
	try {
		// Connect to database
		ConnectionWrapper conn = DbConnectionManager.getInstance().getConnection();
	
		// Create a datafile processor to update the TpaXmit value on the 
		// datafile record in the database
		DbPersistentDatafile dfProcessor = new DbPersistentDatafile(conn);

		// Get the plant destination directory--use processing plant if it exists
		Plant procPlant = routeSchedule.getProcPlant();
		
		// Plant must exist!
		if (procPlant == null) {
			procPlant = routeSchedule.getPlantMagIssue().getPlant();
		}	

		// Get Destination 'Archive' Directory to move processed file to.
		String archiveDir = PropertyBroker.getProperty("DataFileArchiveDirectory");
		if (archiveDir == null) {
			SybilWarningException e = new SybilWarningException 
					("SchedulePlantDataManager: DataFileArchiveDirectory not found " +
						"File not moved to archive directory.");
			LogWriter.writeLog(e);
		}

		// Get Destination directory to stage files (On Wamnet Box)
		String stageDir = PropertyBroker.getProperty("ScheduleOutputStageDir");
		if (stageDir == null) {
			SybilWarningException e = new SybilWarningException 
					("SchedulePlantDataManager: ScheduleOutputStageDir not found " +
						"File not moved to Scheduled Output.");
			LogWriter.writeLog(e);
		}

		// Get Destination directory to transmit file (wamnet directory for plant)
		String xmitDir = PropertyBroker.getProperty("ScheduleOutputFileDir") + procPlant.getXmitDir();
		if (xmitDir == null) {
			SybilWarningException e = new SybilWarningException 
					("SchedulePlantDataManager: ScheduleOutputFileDir not found for plant <" +
						datafile.getProcPlant() + ">. File not moved.");
			LogWriter.writeLog(e);
		}

	// ********************************************************************
	// * Files are sent in a three-step process:
	// *	1) File (df) is COPIED from source to stageDir (i.e. /Wamnet/Sybil)
	// *	2) File (stageDir) is MOVED from stageDir to xmitDir 
	// *		(i.e. /Wamnet/plantspecificdir)
	// *	3) File (df) is MOVED from source to archiveDir.
	// ********************************************************************
		
	// Part 1) COPY file to Wamnet staging directory (stageDir).
		File stageFile = new File(stageDir, dataFileName);
		if (stageFile.exists()) {
			stageFile.delete();
		}	
		boolean success1 = df.copyTo(stageFile);
		if (!success1) {
			SybilWarningException e = new SybilWarningException("SchedulePlantDataManager: Could not COPY file " + 
							df.toString() + " to " + stageFile.toString() + ".  Make sure file is manually copied.");
			LogWriter.writeLog(e);
		}
		/*******************************************************************/		
		//**********Rename MLTIMAIL file back to zip file extension*********

		if (parser == null) {
			parser = new PlantMagIssueFilenameParser();
		}
		
		Hashtable dataElements = parser.parseFilename(dataFileName);
		String dataTypeCode = (String)dataElements.get("dataType");
		
		if (dataTypeCode.equalsIgnoreCase("mltimail")) {
			multiMailFileName = dataFileName;
			dataFileName = dataFileName.substring(0, dataFileName.length() -4);
			}
		
		//*****************End of code to rename MLTIMAIL file**************
		/*******************************************************************/
	
	// Part 2) MOVE file from 'stageDir' to 'xmitDir' directory.
	//	Note: This step actually sends the file to the plant via Wamnet
		File xmitFile = new File(xmitDir, dataFileName);
		if (xmitFile.exists()) {
			xmitFile.delete();
		}
		boolean success2 = stageFile.renameTo(xmitFile);
		if (!success2) {
			SybilWarningException e = new SybilWarningException("SchedulePlantDataManager: Could not MOVE file " + 
							stageFile.toString() + " to " + xmitFile.toString() + ".  Make sure file is manually moved.");
			LogWriter.writeLog(xmitFile.getName());
		} else {
			LogWriter.writeLog("SchedulePlantDataManager: Datafile <" + df.toString() +
					"> sent to <" + xmitFile.toString() + ">.");
			SybilEventManager.logEvent(new SybilFileEvent(SybilFileEvent.E_TransmitFile,
				new Magazine(xmitFile.getName()), datafile,
				"File <" + xmitFile.getName() + "> sent to Plant <" + procPlant.getPlantId() + ">."));
		}

		xmitFile = null;	// This is done to free up stageFile from previous 'copy'
		stageFile = null;	// so that the following 'move' of 'df' will work.
		for (int i=0; i < 50; i++) {
			System.gc();
			rt.gc();
		}
		/*******************************************************************/
		//**********Rename MLTIMAIL file back to zip file extension*********

		if (dataTypeCode.equalsIgnoreCase("mltimail")) {
			dataFileName = multiMailFileName;
		}
		
		//*****************End of code to rename MLTIMAIL file**************
		/*******************************************************************/
	
	// Part 3) MOVE file (df) from 'ToSend' to 'archiveDir' directory (i.e. 'Sent').
		File archiveFile = new File(archiveDir, dataFileName);
		if (archiveFile.exists()) {
			archiveFile.delete();
		}
		boolean success3 = df.renameTo(archiveFile);
		if (!success3) {
			SybilWarningException e = new SybilWarningException("SchedulePlantDataManager: Could not MOVE file " + 
							df.toString() + " to " + archiveFile.toString() + ".  Make sure file is manually moved.");
			LogWriter.writeLog(e);
		}
		// Set last modified time of moved file so that create and modified time are the same.
		else {
			try {
				lastModCurrTime = System.currentTimeMillis();
				lastModOK = archiveFile.setLastModified(lastModCurrTime);
				if(!lastModOK) {
					LogWriter.writeLog("ERROR: SchedulePlantDataManager.sendFile ... " + 
						"Could not set last modified time on " + archiveFile.toString());
				}
			}
			catch (Exception e) {
				LogWriter.writeLog(e);
				LogWriter.writeLog("ERROR: SchedulePlantDataManager.sendFile ... " + 
					"Could not set last modified time on " + archiveFile.toString());
			}
		}
	}
	catch (Exception e) {
		LogWriter.writeLog(e);
	}

	return;
		
}
}
