package sybil.tampa.controller;

import sybil.common.event.*;
import sybil.common.persistence.*;
import sybil.common.util.*;
import sybil.common.model.*;

/**
 *	The manager class for controlling all issue customer persistence.
 *	After an issue customer is parsed by ProcessDataForPlantThread,
 *	an IssueCustomerProcessedEvent is sent to this class, triggering
 *	the persistence process.  This class also maintains a Hashtable
 *	of PersistentIssueCustomer objects, each for a particular destination
 *	as defined by the dest field of IssueCustomer class.
 *
 *	Used in <a href=sybil_requirement_1.4.html#ProcessDataForPlant>Process
 *	Data for Plant</a>, <a href=sybil_requirement_1.4.html#PrepareForRouting>
 *	Prepare Data for Routing</a>, and <a href=sybil_requirement_1.4.html#>
 *	ArchiveData</a>.
 *
 *	@author:		Jun Ying
 */

public class PersistentIssueCustomerManager implements Runnable, IssueCustomerProcessedListener {
	private PersistentIssueCustomerFactory picFactory;
	private java.util.Vector events;
	private PersistentIssueCustomer persis;
	private boolean stop;
	int saved = 0;
	
	private java.util.Hashtable persists;
	private java.util.Vector threads;
/**
 * 	Initializes the object.
 *		@param	dah	sybil.common.event.DataArrivedHandler
 					The true target of the DataArrivedEvent
 *		@see		sybil.common.event.DataArrivedHandler
 */
public PersistentIssueCustomerManager () {
	events = new java.util.Vector();
	persists = new java.util.Hashtable();
	threads = new java.util.Vector();
	
	String persistMethod = PropertyBroker.getProperty("IssueCustomerPersistenceMethod");
 	if (persistMethod != null) {
		picFactory = new PersistentIssueCustomerFactory(persistMethod);
	} else {
		picFactory = new PersistentIssueCustomerFactory();
	}	
}
/**
 * Since multiple ProcessDataForPlantThread objects can be sending
 * IssueCustomer objects to the same PersistentIssueCustomer object,
 * this method keeps track any active threads.  Only when all active
 * threads have removed themselves from the Vector, the
 * PersistentIssueCustomer can be closed out.  There would be a problem
 * if there are always threads active.  Currently it is not a problem
 * since we are assuming only one thread can write to one
 * PersistentIssueCustomer object.
 *
 * @param t java.lang.Thread
 */
public void addThread(Thread t) {
	threads.addElement(t);
	return;
}
/**
 *		This method finishes a particular PersistentIssueCustomer
 *		to close up any open files as well as write the message.ctl
 *		file and the customer.grp file.
 *
 *		@param dest java.lang.String
 */
public void finish(String dest, java.util.Vector mp, MailStripContentParameters p,
	int index) {
	
	PersistentIssueCustomer persist = (PersistentIssueCustomer) persists.get(dest);
	
	if (persist == null) {
		persist = picFactory.createPersistentIssueCustomer(dest);
		persist.setMailStripContentParam(p, index);
	}	

	persist.writeMessageParms(mp);

	persist.finish();
	
	persists.remove(dest);
}
/**
 *		Find the PersistentIssuecustomer for the IssueCustomer's dest
 *		in the Hashtable and save it.
 */

public void issueCustomerProcessed(IssueCustomerProcessedEvent icpe) {
	sybil.common.model.IssueCustomer ic = (sybil.common.model.IssueCustomer) icpe.getIssueCustomer();
	String dest = ic.getDest();
	synchronized (events) {
		//events.addElement(icpe);
		// notify so that the event handling thread will restart
		//events.notify();
		if (persists.containsKey(dest)) {
			((sybil.common.persistence.PersistentIssueCustomer) persists.get(dest)).saveIssueCustomer(icpe.getIssueCustomer());
		}
		else {
			PersistentIssueCustomer p = picFactory.createPersistentIssueCustomer(dest);
			p.setMailStripContentParam(icpe.getMailStripContentParam(), icpe.getMSParamIndex());
			persists.put(dest, p);
			p.saveIssueCustomer(icpe.getIssueCustomer());
		}	
	}	

}
/**
 * Removes a finished thread.
 * @param t java.lang.Thread
 */
public void removeThread(Thread t) {
	threads.removeElement(t);
	if (threads.size() == 0) {
		setStop(true);
	}	
	return;
}
/**
 *		This thread queues up the events and handle them in a serial
 *		fashion.
 */
public void run() {
	int tempSaved = 0;
	Runtime rt = Runtime.getRuntime();
	java.util.Vector tempEvents = null;
	while (true) {
		synchronized (events) {
			if (tempEvents != null) {
				tempEvents.removeAllElements();
			}	
			tempEvents = (java.util.Vector) events.clone();
			events.removeAllElements();
			//System.out.println("queue emptied.");
			//System.out.println(tempEvents.size() + " copied to be processed.");
			// if there is no event in the event queue, wait
			while (tempEvents.size() == 0) {
				if (stop) {
					//System.out.println(tempSaved + " saved.");
					persis.finish();
					return;
				}	
				try {
					events.wait();
				} catch (InterruptedException ie) {
				}
				// when notified, move events from the queue
				// to the temp queue
				if (tempEvents != null) {
					tempEvents.removeAllElements();
				}	
				tempEvents = (java.util.Vector) events.clone();
				events.removeAllElements();
				//System.out.println("queue emptied.");
				//System.out.println(tempEvents.size() + " copied to be processed.");
			}
			//rt.gc();
		}
		
		// pass the events to the handlers for processing
		// may take time					
		int size = tempEvents.size();
		for (int i = 0; i < size; i++) {
			IssueCustomerProcessedEvent tempEvent = 
							((IssueCustomerProcessedEvent) tempEvents.elementAt(i));
			persis.saveIssueCustomer(tempEvent.getIssueCustomer());
			//System.out.println("processed " + (i + 1) + " in the temp queue.");
			++saved;
			++tempSaved;
		}
		/*
		if (tempSaved > 500) {
			System.gc();
			tempSaved = 0;
		}
		*/
	}
}
/**
 * This method was created by a SmartGuide.
 * @param stop boolean
 */
private void setStop(boolean stop) {
	java.util.Enumeration p = persists.elements();
	while (p.hasMoreElements()) {
		((PersistentIssueCustomer) p.nextElement()).finish();
	}	
	this.stop = stop;
	synchronized (events) {
		events.notify();
	}	
	return;
}
public void storageBookProcessed(IssueCustomerProcessedEvent icpe) {
	sybil.common.model.StoragePostalBook spb = (sybil.common.model.StoragePostalBook) icpe.getStoragePostalBook();
	String dest = spb.getDest().intern();
	synchronized (events) {
		if (persists.containsKey(dest)) {
			((sybil.common.persistence.PersistentIssueCustomer) persists.get(dest)).saveStoragePostalBook(spb);
		}
		//saveIssueCustomer(icpe.getIssueCustomer());
		else {
			PersistentIssueCustomer p = picFactory.createPersistentIssueCustomer(dest);
			p.setMailStripContentParam(icpe.getMailStripContentParam(), icpe.getMSParamIndex());
			persists.put(dest, p);
			p.saveStoragePostalBook(spb);
		}	
		//System.out.println("received " + icpe.getDataFile());
	}	

}
}
