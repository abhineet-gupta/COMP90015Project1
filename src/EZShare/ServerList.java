/*
 * Distributed Systems
 * Group Project 1
 * Sem 1, 2017
 * Group: AALT
 * 
 * Class for managing list of servers that the current server is aware of
 */

package EZShare;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ThreadLocalRandom;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class ServerList {
	private static JSONArray serverList = new JSONArray(); //stores the list of servers
	private static Integer MIN_PORT = 1024;    //0-1023 are reserved for system
	private static Integer MAX_PORT = 65535;
	
	//Update current server list with an incoming list i.e. a union of the two lists
	@SuppressWarnings("unchecked")
	public synchronized static void update(JSONArray newList, String hostname, int hostport) 
			throws ClassCastException, UnknownHostException, NumberFormatException, serverException {

	    //For each server in incoming list
	    for (Object newServerObject : newList) {
			JSONObject newServerJSON = (JSONObject) newServerObject;
			
			//Validate
			if (!newServerJSON.containsKey("hostname") || !newServerJSON.containsKey("port")) {
                throw new serverException("missing resourceTemplate");
            }
			String newHostname = (String) newServerJSON.get("hostname");
			InetAddress.getByName(newHostname);
			int newPort = Integer.parseInt(newServerJSON.get("port").toString());
			if (newPort < MIN_PORT || newPort > MAX_PORT) throw new serverException("invalid server record");
			
			boolean safeToAdd = true;
			//don't add if the server is self
			if (newHostname.equals(hostname) && newPort == hostport){
			    safeToAdd = false;
			}
			
			//don't add if the server already exists in list
			for (Object oldEle : serverList) {
				JSONObject oldServer = (JSONObject) oldEle;
				String oldHostname = (String) oldServer.get("hostname");
	            int oldPort = Integer.parseInt(oldServer.get("port").toString());
	            
				if(newHostname.equals(oldHostname) && newPort == oldPort){
	                safeToAdd = false;
	            }
			}
			
			if (safeToAdd) serverList.add(newServerJSON);
		}		
	}
	
	//Select a random server from the list
	public static synchronized JSONObject select() {
	    if (serverList.size() > 0) {
			int random = ThreadLocalRandom.current().nextInt(0, serverList.size());
			JSONObject randomServer = (JSONObject) serverList.get(random);
			return randomServer;
		}
		return null;
	}
	
	public static int getLength() {
		return serverList.size();
	}
	
	public static synchronized void remove(JSONObject server) {
		serverList.remove(server);
	}

    public static JSONArray getCopyServerList() {
        if (serverList.size() > 0) return (JSONArray) serverList.clone();
        return null;
    }
}
