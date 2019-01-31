package sybil.tampa.controller;

/**
 * This type was created in VisualAge.
 */
import java.util.zip.*;
import sybil.common.util.*;
import sybil.common.event.*;
import sybil.common.model.*;
import java.util.*;
import java.io.*;
import sybil.common.persistence.*;

public class ProcessSupplementDataThread extends Thread{
	private String dest;
	private String zipFileName;
	private ZipFilePersistent zfp;
	private String inputFileName;
	private String prographDir = null;
	private boolean sendToPrograph = false;
/**
 * ProcessSupplementData constructor comment.
 */
public ProcessSupplementDataThread(MailStripContentParameters p, int fileNum) {
	super();

	String outputDir = null;
	String inputDir = null;
	
	setDestination(p, fileNum);
	
	if ((outputDir = PropertyBroker.getProperty("CustomerDataFileOutputDirectory")) == null) {
		SybilWarningException e = new SybilWarningException(this.getName() + ": CustomerDataFileOutputDirectory is not defined.");
		LogWriter.writeLog(e);
		return;
	}

	File dirFile = new File(outputDir);
	if (!dirFile.exists() || !dirFile.isDirectory()) {
		SybilWarningException e = new SybilWarningException(this.getName() +": " + outputDir + " does not exist or is not a directory.");
		LogWriter.writeLog(e);
		return;
	}

	if ((inputDir = PropertyBroker.getProperty("CustomerDataFileInputDirectory")) == null) {
		SybilWarningException e = new SybilWarningException(this.getName() + ": CustomerDataFileInputDirectory is not defined.");
		LogWriter.writeLog(e);
		return;
	}

	inputFileName = inputDir + p.getdatasetNameOfFile(fileNum);
	File inputFile = new File(inputFileName);
	if( !inputFile.exists()) {
		SybilWarningException e = new SybilWarningException(this.getName() +": input file name <" + inputFileName + "> does not exist.");
		LogWriter.writeLog(e);
		return;
	}

	if( (prographDir = PropertyBroker.getProperty("PrographDirectory")) != null) {
		dirFile = new File( prographDir);
		if( !dirFile.exists() || !dirFile.isDirectory()) {
			LogWriter.writeLog("PrographDirectory: <"+dirFile.getPath() + "> is not defined properly!");
			prographDir = null;
		} else {
			String sourceLabelType = p.getSourceLabelType(fileNum).toLowerCase();
			if( sourceLabelType.equalsIgnoreCase("pkgsum") ||
				sourceLabelType.equalsIgnoreCase("rollsum")){

					sendToPrograph = true;
			}
		}
	}
	
	zipFileName = outputDir + dest;

	// initialize zip file persistance object
	zfp = new ZipFilePersistent(zipFileName);

}
/**
 * This method was created in VisualAge.
 * @return java.lang.String
 */
public String getDestination() {
	return dest;
}
/**
 * This method was created in VisualAge.
 */
public void run() {
	File f = null;
	
	f = new File(inputFileName);
	if (f.exists()) {
		try {
			zfp.addFile(f);
		} catch (SybilWarningException e) {
			LogWriter.writeLog(e);
		}
	} else {
		SybilWarningException e = new SybilWarningException(this.getName() + ":<ERROR!> Supplement file " + f.getName() + " could not be found!!!");
		LogWriter.writeLog(e);
	}

	zfp.finish();

	if( prographDir == null || !sendToPrograph)
		return;
		
	SybilFile sf = new SybilFile (zfp.getZipFileName());

	if( !sf.exists()){
		LogWriter.writeLog( "Error! <" + sf.getPath() + 
			"> does not exist!  File has not been copied to <" + 
			prographDir +">.");
		return;
	}
	
	File nf = new File(prographDir, sf.getName());
	
	if( nf.exists()){
		if( !nf.delete()) {
			LogWriter.writeLog( "Error! Could not remove the old version " +
				"of the file in <" + nf.getPath() + ">. Please copy the new version" +
				" of the file to <" + prographDir + "> manually!");
			return;
		}
	}
	
	if(!sf.copyTo(nf)){
		LogWriter.writeLog("Error! <" + sf.getPath() + 
			"> could not be copied to <" + prographDir + ">!");
	}
	
	return;
}
/**
 * This method was created by a SmartGuide.
 * @return java.lang.String
 */
public void setDestination(MailStripContentParameters p, int fileNum) {
	
	this.dest = p.getMagazineCode().toLowerCase()+".i"+
				p.getIssueNumber().toLowerCase()+"."+
				p.getPlantRepresentedByFile(fileNum).toLowerCase()+"."+
				p.getSourceLabelType(fileNum).toLowerCase()+"."+
				p.getSourceProcessType(fileNum).toLowerCase()+"."+
				p.getSourceDataType(fileNum).toLowerCase()+"."+
				p.getWeekNumber().toLowerCase()+".zip";
	return;
}
/**
 * Returns a String that represents the value of this object.
 * @return a string representation of the receiver
 */
public String toString() {
	// Insert code to print the receiver here.
	// This implementation forwards the message to super. You may replace or supplement this.
	return super.toString();
}
}
