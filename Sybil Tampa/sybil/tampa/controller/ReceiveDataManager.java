package sybil.tampa.controller;

/**
 *	ReceiveDataManager receives DataArrivedEvent from the
 *	DataArrivedChecker, parses out the MailStripContentParameter
 *	information, and creates a ProcessDataForPlantThread to handle
 *	the data processing.  It implements the DataArrivedHandler
 *	interface because the DataArrivedAdapter need to be able to
 *	correctly forward the queued DataArrivedEvent.
 *
 *  Used in <a href=sybil_requirement_1.4.html#TampaReceiveData>
 *  Tamp Receive Data</a>.
 *
 *	@author Jun Ying
 */
//============================================================
//Change Log
//============================================================
//02/04/2002
//	Applied NW 1.1 changes to W 1.0.
//============================================================
import java.util.*;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.ParseException;

import sybil.common.model.*;
import sybil.common.event.*;
import sybil.common.log.*;
import sybil.common.util.*;

import sybil.common.persistence.DbPersistentMag;
import sybil.common.persistence.DbPersistentMagIssue;
import sybil.common.persistence.DatabaseOperationFailedException;
import sybil.common.persistence.DbConnectionManager;
import sybil.common.persistence.ConnectionWrapper;

public class ReceiveDataManager extends ReceiveInputManager implements sybil.common.event.DataArrivedHandler {

	PersistentIssueCustomerManager persisMgr;
	LogEventDispatcher dispatcher;
	Mag magInfo = null;
	String OMSMags = null;
/**
 * This method was created by a SmartGuide.
 */
public ReceiveDataManager () {

	super("InputFileDir", "InputFileExt");

	persisMgr = new PersistentIssueCustomerManager();
	// uncomment the next two lines for a separate persistence thread
	// see also in PersistentIssueCustomerManager's issueCustomerProcessed
	// method
	
	//t = new Thread(custArchiver);
	//t.start();
	
	dispatcher = new LogEventDispatcher();

	String prop = null;	
	if( (prop = PropertyBroker.getProperty("OMSMagazines")) == null) {
		SybilWarningException e = new SybilWarningException(
				"ReceiveDataManager(): OMSMagazines is not defined.");
		LogWriter.writeLog(e);
		return;
	}

	OMSMags = prop.trim().toLowerCase();		
		
}
public void addLogListener(LogListener ll) {
	dispatcher.addLogListener(ll);
}
/**
 * This method was created by a SmartGuide.
 * @param theMagCode java.lang.String
 * @param theIssueNum java.lang.String
 * @param theCoverDate java.util.Date
 */
public void createMagIssue(String theMagCode, int theIssueNum, Date theCoverDate) {
	MagIssue magIssue = null;
	boolean success = true;
	
	// Load magIssue if it exists
	try {
		// Connect to database and begin a transaction
		ConnectionWrapper conn = DbConnectionManager.getInstance().getConnection();

		DbPersistentMag mProcessor = new DbPersistentMag(conn);	
		magInfo = mProcessor.loadUsingMagCode(theMagCode);

		// If the magazine does not exist then error out
		if (magInfo == null) {
			throw new NullPointerException("Magazine does not exist in the database for \r\n" +
							", Mag Code=" + theMagCode);
		}
				
		DbPersistentMagIssue miProcessor = new DbPersistentMagIssue(conn);
	
		magIssue = miProcessor.loadUsingMagCodeIssueNum(theMagCode, theIssueNum);

	// Begin a transaction
		miProcessor.beginTransaction();

	// If magIssue does not exist then create it
		if (magIssue == null) {
			// Load mag--this should always exist!!

		// Create magIssue entity
			magIssue = miProcessor.create();
			magIssue.setMagKey(magInfo.getMagKey());
			magIssue.setIssueNum(theIssueNum);
		}

		MagIssue miClone = (MagIssue)miProcessor.register(magIssue);
		miClone.setCoverDate(theCoverDate);


	// Save the record to the database
		miProcessor.commitTransaction();
		
		// Release database connection
		DbConnectionManager.getInstance().freeConnection(conn);
	}
	catch (Exception e) {
		LogWriter.writeLog(e);
		success = false;
	}	

	// If something failed along the way then warn user
	if (!success) {
		sybil.common.util.SybilWarningException e =
			new sybil.common.util.SybilWarningException("Could note create Magazine-Issue" + 
						", MagCode=" + theMagCode + ", IssueNum=" + theIssueNum + ".");
			LogWriter.writeLog(e);
	}

	return;
}
/**
 * 	Returns the name of the method that would handle the
 *		DataArrivedEvent.  That method has to take only one
 *		argument: DataArrivedEvent
 *		@return java.lang.reflect.Method
 */
public String getEventHandlerMethod() {
	return "processData";		
}
/**
 * This method was created by a SmartGuide.
 * @return java.util.Vector
 * @param s java.lang.String
 */
private java.util.Vector parseFileName(String fName) {

	java.util.Vector v = new java.util.Vector();
	java.util.StringTokenizer st = new java.util.StringTokenizer(fName, ".");
	for (int i = 0; i < st.countTokens(); i++) {
		v.addElement(st.nextToken());
	}	

	return v;
}
/**
 *		The main functional method of the class.  It takes the file name
 *		inside the DataArrivedEvent and parses the file for parameters.
 *		For each data file it finds in the parameter file, it creates a
 *		ProcessDataForPlantThread to process it.
 *		@param dae sybil.common.event.DataArrivedEvent
 */
public void processData(DataArrivedEvent dae) {
	
	String paramFileName = null;		// Mailstrip parameter file currently being processed	
		
	try {
		MailStripContentParameters param = new MailStripContentParameters();
		MailStripParameterParser parser = new MailStripParameterParser();
		paramFileName = dae.getDataFile();

/*		// Start of OMS2MSG code
		String checkFile = dae.getDataFile().toUpperCase();
		int i1 = checkFile.indexOf(".OMS2MSG.");
		if (i1 < 0) {
		}
		else {
			try {
				LogWriter.writeLog("ReceiveDataManager.processData: <" + checkFile + ">. OMS2MSG index = " + i1);
				java.io.File f1 = new java.io.File(paramFileName);
				String fName1 = f1.getName();
				String archiveDir1 = PropertyBroker.getProperty("ArchivedMailstripParmsDir");
				java.io.File newFile1 = new java.io.File(archiveDir1, fName1);
				boolean success1 = f1.renameTo(newFile1);
				if (!success1) {
					sybil.common.util.SybilWarningException e1 = new sybil.common.util.SybilWarningException("Could not move file " + 
						f1.toString() + " to " + newFile1.toString() + ".  Make sure file is manually moved.");
					LogWriter.writeLog(e1);
					return;
				}
				else {
					LogWriter.writeLog("ReceiveDataManager.processData: Moved file " + f1.toString() + 
						" to " + newFile1.toString() + " without processing.");
					return;
				}
			}
			catch (Exception e2) {
				LogWriter.writeLog(e2);
				return;
			}
		}
	*/	// End of OMS2MSG code
		LogWriter.writeLog("Into RDM-Process DATA");
		parser.parseFile(paramFileName, param);
		String mag = param.getMagazineCode();
		String issue = param.getIssueNumber();
		String week = param.getWeekNumber();
		int issueNum = Integer.parseInt(issue);
		String paramDate = param.getIssueDate();
		if ((paramDate.substring(0, 2).equals("00")) ||
			(paramDate.substring(3, 5).equals("00"))) {
			sybil.common.util.SybilWarningException e =
				new sybil.common.util.SybilWarningException("Warning! Could not process " + 
					"mailparm file <" + paramFileName + "> due to invalid issue date <" +
					paramDate + ">.");
			LogWriter.writeLog(e);
			return;
		}
		SimpleDateFormat df = new SimpleDateFormat("MM/dd/yy");
		Date issueDate = df.parse(paramDate);
		int numOfFiles = param.getNumFiles();

		LogWriter.writeLog("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
		LogWriter.writeLog("ReceiveDataManager: Processing MailstripContentParameters for:");
		LogWriter.writeLog("                    Mag <" + mag + ">, Issue <" + issue +
								">, Number of files <" + numOfFiles + ">.");
		LogWriter.writeLog("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");

		// Make sure a Mag-Issue record exists in the database for this mailstrip
		magInfo = new Mag();		// Create new magInfo each time a mailparm is processed.
		createMagIssue(mag, issueNum, issueDate);

		// Loop through all files in the mailstrip file and process them
		for (int i = 0; i < numOfFiles; i++) {
			if( (i == 0) && 
				((param.getSourceDataType(i).equalsIgnoreCase("OMS2"))||(param.getSourceDataType(i).equalsIgnoreCase("OMS2MSG"))) && 
				(OMSMags != null) && 
				( OMSMags.indexOf( param.getMagazineCode().trim().toLowerCase()) == -1)){
				
				break;
			}
			
			if( param.getSourceDataType(i).equals("MAILDAT")){
				try{
					ProcessMaildatThread pmd = new ProcessMaildatThread(param, i);
					pmd.start();
					pmd=null;
				} catch (Exception e) {
					LogWriter.writeLog(e);
				}			
			} else 	if( param.getSourceDataType(i).equals("SYBILDAT")){
				try{
					ProcessSybildatThread psd = new ProcessSybildatThread(param, i);
					psd.start();
					psd=null;
				} catch (Exception e) {
					LogWriter.writeLog(e);
				}
			} else 	if( param.getSourceDataType(i).equals("PDFUSPS")){
				try{

					String inputFilename = param.getdatasetNameOfFile(i);
					String InputFilePath = (String)PropertyBroker.getProperty("CustomerDataFileInputDirectory");
					InputFilePath = InputFilePath.concat(inputFilename);

					LogWriter.writeLog("Processing... "+InputFilePath);					

					String OutputFilePath = (String)PropertyBroker.getProperty("ReportPdfDirectoryStage");

					String OutputFilename = param.getMagazineCode().toLowerCase()+".i"+
					param.getIssueNumber().toLowerCase()+"."+
					param.getPlantRepresentedByFile(i).toUpperCase()+".OMS_"+
					(param.getSourceLabelType(i).toUpperCase()).substring(0,(param.getSourceLabelType(i)).length()-1);

					if(param.getSourceLabelType(i).equals("STDDOCS")){
						OutputFilename = OutputFilename.concat("_SACK");
					}else{
						OutputFilename = OutputFilename.concat("_PALLET");						
					}

					OutputFilename = OutputFilename + "."+week+"."+ param.getSourceDataType(i).substring(3,param.getSourceDataType(i).length()).toUpperCase()+".pdf";					
					
						
					OutputFilePath = OutputFilePath.concat(OutputFilename);

					String FinalDest = (String)PropertyBroker.getProperty("ReportPdfDirectory");
					FinalDest = FinalDest.concat(OutputFilename);
						
					LogWriter.writeLog("Generating... "+OutputFilePath);
					
					sybil.common.model.PDFDocument pdfdoc = new sybil.common.model.PDFDocument(InputFilePath,OutputFilePath,FinalDest,"Lanscape","Courier",8);
					pdfdoc.run();
					pdfdoc.close();

					LogWriter.writeLog(OutputFilePath +" renamed to "+FinalDest);					

				} catch (Exception e) {
					LogWriter.writeLog(e);
				}			
					
			}else if( param.getSourceDataType(i).equalsIgnoreCase("REPORTS") || 
						param.getSourceDataType(i).equalsIgnoreCase("LORBOOK2") ||
						param.getSourceDataType(i).equalsIgnoreCase("MLTIMAIL") ||
						param.getSourceDataType(i).equalsIgnoreCase("FRSTCLAS")) {
				try{
					ProcessSupplementDataThread psd= new ProcessSupplementDataThread( param, i);
					psd.start();
					psd = null;
				}catch( Exception e) {
					LogWriter.writeLog(e);
				}
			} else {
				ProcessDataForPlantThread t = new ProcessDataForPlantThread(param.getdatasetNameOfFile(i), 
						param.getSourceDataType(i), param.getNumberOfRecordsInFile(i), param.getLORBooks());
				t.setPersistenceManager(persisMgr);
				t.setMag(magInfo);
				t.setPlant(param.getPlantRepresentedByFile(i));
				t.setMessageParms (param.getmsgParms(i));
				t.setMagazine(mag);
				t.setIssueNumber(issue);
				t.setProcessType(param.getSourceProcessType(i));
				LogWriter.writeLog("processType = "+  param.getSourceProcessType(i));
				t.setLabelType(param.getSourceLabelType(i));
				t.setMailStripContentParam(param, i);
				t.setWeekNumber(week);	
				t.start();
			
				// set the variable to point to null so that the garbage
				// collector would collect the thread after it's done.
				t = null;
			} // else
		}	
	
		// Move the current mailstrip parm file to the 'processed' directory.
		magInfo = null;		// eliminate magInfo 'til next time.
		java.io.File f = new java.io.File(paramFileName);
		String fName = f.getName();
		String archiveDir = PropertyBroker.getProperty("ArchivedMailstripParmsDir");
		java.io.File newFile = new java.io.File(archiveDir, fName);
		boolean success = f.renameTo(newFile);
		if (!success) {
			sybil.common.util.SybilWarningException e =
				new sybil.common.util.SybilWarningException("Could not move file " + f.toString() + 
							" to " + newFile.toString() + ".  Make sure file is manually moved.");
			LogWriter.writeLog(e);
		}
// Prior to exiting, clean up old issues of data from Sybil Tampa
		Magazine magazine = new Magazine(mag, issue);
		magazine.setDeliveryType("Tampa");
		removeOldIssue(magazine);
	} catch (Exception e) {
		LogWriter.writeLog(e);
	}		
	return;
}
public void removeLogListener(LogListener ll) {
	dispatcher.removeLogListener(ll);
}		
/**
 * This method was created by a SmartGuide.
 * @param m sybil.common.model.Magazine
 */
private void removeOldIssue(Magazine m) {

//	First, determine current issue being processed, and how old an issue must
//	be in order to be purged.
	int issue = Integer.parseInt(m.getIssue());		// Current Issue being processed
	int backlogIssue = Integer.parseInt(PropertyBroker.getProperty("BackLog", "2"));
	String magCode = m.getMagCode();

//	All old issues for the current magazine will be deleted
//	from the CustomerDataFileInputDirectory.
	String dir = PropertyBroker.getProperty("CustomerDataFileInputDirectory");
	java.io.File dirFile = new java.io.File(dir);
	String [] list = dirFile.list();

//	Loop through each file in the directory list.  If the file
//	is older than the oldest issue to be retained, delete it.
	for (int i = 0; i < list.length; i++) {
		java.io.File f = new java.io.File(dir, list[i]);
		java.util.Vector fn = parseFileName(f.getName());
		String mc = (String) fn.elementAt(0);
		if (mc.equals(magCode)) {
			String tmp = (String) fn.elementAt(1);
			String issueNumString = tmp.substring(1, tmp.length());
			Magazine mag = new Magazine((String)fn.elementAt(0), issueNumString);
			mag.setDeliveryType("Tampa");
			if (mag.olderThan(m) >= backlogIssue) {
				f.delete();
			}
		}	
	}

//	Next, all old issues for the current magazine will be deleted
//	from the ProcessInputFileDir (the 'mailstrip parameter' files).
	dir = PropertyBroker.getProperty("ArchivedMailstripParmsDir");
	dirFile = new java.io.File(dir);
	list = dirFile.list();

//	Again, loop through each file in the directory list.  If the file
//	is older than the oldest issue to be retained, delete it.
	for (int i = 0; i < list.length; i++) {
		java.io.File f = new java.io.File(dir, list[i]);
		java.util.Vector fn = parseFileName(f.getName());
		String mc = (String) fn.elementAt(0);
		if (mc.equals(magCode)) {
			String tmp = (String) fn.elementAt(1);
			String issueNumString = tmp.substring(1, tmp.length());
			Magazine mag = new Magazine((String)fn.elementAt(0), issueNumString);
			mag.setDeliveryType("Tampa");
			if (mag.olderThan(m) >= backlogIssue) {
				f.delete();
			}
		}	
	}
	
	return;
}
}
