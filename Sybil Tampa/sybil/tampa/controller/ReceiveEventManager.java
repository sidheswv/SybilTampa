package sybil.tampa.controller;

/**
 * This type was created in VisualAge.
 */

 import java.util.*;
 import sybil.common.event.*;
 import sybil.common.util.ReceiveInputManager;
 import java.io.*;
 import sybil.common.util.*;

 public class ReceiveEventManager extends ReceiveInputManager 
 	implements DataArrivedHandler{
 
/**
 * ReceiveEventManager constructor comment.
 */
public ReceiveEventManager() {
	super("EventFileDir", "EventFileExt");
}
/**
 * This method was created in VisualAge.
 * @return java.lang.String
 */
public String getEventHandlerMethod() {
	return "processData";
}
/**
 * This method was created in VisualAge.
 * @param dae DataArrivedEvent
 */
public void processData(DataArrivedEvent dae) {
	Object obj;
	FileInputStream fis;
	ObjectInputStream ois;

	try {
		File f = new File(dae.getDataFile());
		fis = new FileInputStream(f);
		ois = new ObjectInputStream(fis);

//		LogWriter.writeLog("Processing event file: " + f.toString());

		// Check to see if there are any data to read in from the file
		// (not the input stream).
		while (fis.available() > 0) {
			try {
				obj = ois.readObject();
				String type = obj.getClass().getName().trim();
				if (type.equalsIgnoreCase("SybilErrorEvent") || type.equalsIgnoreCase("sybil.common.event.SybilErrorEvent")) {
					SybilEventManager.logEvent((SybilErrorEvent) obj);
				} else if (type.equalsIgnoreCase("SybilFileEvent") || type.equalsIgnoreCase("sybil.common.event.SybilFileEvent")) {
					SybilFileEvent e = (SybilFileEvent) obj;
//					e.printAll();
					SybilEventManager.logEvent(e);
				} else {
					SybilEventManager.logEvent((SybilBusinessEvent) obj);
				}
			} catch (EOFException eofe) {
				LogWriter.writeLog(eofe);
			} catch (Exception e) {
				LogWriter.writeLog(e);
				break;
			}	// try
		}	// while
		ois.close();
		fis.close();
		f.delete();
	} catch (IOException ioe) {
		LogWriter.writeLog(ioe);
	}
	return;
}
} 
