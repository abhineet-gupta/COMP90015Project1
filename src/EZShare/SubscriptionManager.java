/*
 * Distributed Systems
 * Group Project 2
 * Sem 1, 2017
 * Group: AALT
 * 
 * Manages notifications for subscriptions when new resource/server added
 */


package EZShare;

import java.io.*;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import org.json.simple.JSONArray;

public class SubscriptionManager {
	private static List<Subscription> subList = new ArrayList<Subscription>();
	
	public static synchronized void addSubscription(Subscription newSub) {
			subList.add(newSub);
	}
	
	public static void allSubMatch(Resource newRes) throws IOException {
		for(Subscription sub : subList) {
			sub.matchNewResource(newRes);
		}
	}
	
	public static void newServerCome(JSONArray newServerList, Boolean secure) {
		for(Subscription sub : subList) {
			if (sub.getSecure() == secure && sub.getRelay()){
			    sub.newRelay(newServerList);
			}
		}
	}
	
	public static void removeSubscription(Subscription oldSub) {
		if(!subList.remove(oldSub)) {
            System.out.println(new Timestamp(System.currentTimeMillis())
                    + " - [ERROR] - Could not remove subscription");
		};
	}
}
