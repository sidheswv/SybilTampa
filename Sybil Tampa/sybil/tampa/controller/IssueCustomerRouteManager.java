package sybil.tampa.controller;

import sybil.common.model.*;

/**
 *	This class encapsulates any business logic for routing customers.
 *	Due to special issue and special equipment requirement, a customer
 *	is sometimes routed to a different plant from the orignally assigned.
 *
 *	@author Jun Ying
 */
 
public class IssueCustomerRouteManager {

/**
 * This method was created by a SmartGuide.
 * @return java.lang.String
 * @param ic IssueCustomer
 */
public static String determineDestination(ProcessDataForPlantThread t, IssueCustomer ic) {

	String mag = t.getMagazine().toLowerCase();
	String issue = t.getIssueNumber().toLowerCase();
	String plant = t.getPlant().toLowerCase();
	String label = t.getLabelType().toLowerCase();
	String procType = t.getProcessType().toLowerCase();
	String inputDataType = t.getFileType().toLowerCase();
	String weekNumber = t.getWeekNumber().toLowerCase();
	String fileType = null;
	if (inputDataType.equals("gtrf") || inputDataType.equals("ofce") ||
		 inputDataType.equals("oms2") || inputDataType.equals("gbin")) {
		fileType = "cust";
	} else {
		fileType = inputDataType;
	}	



		String dest = mag + ".i" + issue + "." + plant + "." + label + "." + procType + "."  
		+ fileType + "." + weekNumber ;	

	ic.setDest(dest);

	return dest;
}
public static String determineDestination(ProcessDataForPlantThread t, StoragePostalBook b) {

	String mag = t.getMagazine().toLowerCase();
	String issue = t.getIssueNumber().toLowerCase();
	String plant = t.getPlant().toLowerCase();
	String label = t.getLabelType().toLowerCase();
	String procType = t.getProcessType().toLowerCase();
	String weekNumber = t.getWeekNumber().toLowerCase();
	String fileType = t.getFileType().toLowerCase();

	String dest = mag + ".i" + issue + "." + plant + "." + label + "." + procType + "."  
		+ fileType + "." + weekNumber ;	

	b.setDest(dest);

	return dest;
}
}
