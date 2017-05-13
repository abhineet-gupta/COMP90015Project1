/*
 * Distributed Systems
 * Group Project 1
 * Sem 1, 2017
 * Group: AALT
 * 
 * Class that defines serverException and error message
 */

package EZShare;

@SuppressWarnings("serial")
public class serverException extends Exception {
	private String errorMsg;
	
	public serverException(String errorMsg) {
		this.errorMsg = errorMsg;
	}
	
	public String toString() {
		return errorMsg;
	}
}
