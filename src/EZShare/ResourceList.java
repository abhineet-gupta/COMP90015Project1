/*
 * Distributed Systems
 * Group Project 1
 * Sem 1, 2017
 * Group: AALT
 * 
 * This class keeps track of which resources exist on this server.
 * It implements the basic list functions:
 *      [add, remove, indexOf, querying for various applications] 
 *      using 'synchronised' for concurrency protection.
 */

package EZShare;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class ResourceList {
	private static ArrayList<Resource> resourceList = new ArrayList<Resource>();
  
	private synchronized static boolean modifyReourceList (boolean operation, Resource resource) throws serverException {
		//true for add, false for remove 
		if (operation) {
			return resourceList.add(resource);
		} else {
			return resourceList.remove(resource);
		}
	}
	
	//thread safe because of "synchronized"
	public static void addResource(Resource newResource) throws serverException {
		//Check if resource already exists with same channel and URI
		Resource match = queryForChannelURI(newResource);
		
		//if no existing resource channel and URI matches... 
		if (match == null) {
		    //...then add it.
			if (!modifyReourceList(true, newResource)) {
				throw new serverException("cannot publish resource");
			}
			//otherwise...
		} else { 
		    //...check if their owners are the same. 
		        //If not, deny publish request.
			if (!match.getOwner().equals(newResource.getOwner()))
				throw new serverException("cannot publish resource");
			
			   //...else: remove existing resource with that PK and add new one 
			if (!modifyReourceList(false, match) || !modifyReourceList(true, newResource)) 
				throw new serverException("cannot publish resource");
		}
	}
	
	public static void removeResource(Resource oldResource) throws serverException {
		Resource match = queryForPK(oldResource);
		if (match == null) {
			throw new serverException("cannot remove resource");
		} else {
			if (!modifyReourceList(false, match)) throw new serverException("cannot remove resource");
		}
	}

	//Check if resource exists based on it Primary Key (URI, owner, channel)
	public static Resource queryForPK(Resource re) {
	    //Check if resource already exists...
	    Resource match = null;
		
		for (int i = 0; i < resourceList.size(); i++) {    //...return its position in the list if it does.
			if (re.getChannel().equals(resourceList.get(i).getChannel()) && 
				re.getOwner().equals(resourceList.get(i).getOwner()) &&
				re.getUri().equals(resourceList.get(i).getUri())) {
				match = resourceList.get(i);
			}
		}
		return match;
	}
	
	public static Resource queryForChannelURI(Resource re) {
	    Resource match = null;
	    
		for (int i = 0; i < resourceList.size(); i++) {    //...return its position in the list if it does.
			if ((re.getChannel().equals(resourceList.get(i).getChannel())) &&
					(re.getUri().equals(resourceList.get(i).getUri()))) {
				match = resourceList.get(i);
			}
		}
		return match;
	}
	
	public int getSize() {
		return resourceList.size();
	}

	//Return a list of resources that match the 'query' criteria
    public static List<Resource> queryForQuerying(Resource in_res) {
        List<Resource> results = new ArrayList<>();
        
        // Query current resource list for resources that match the template
        for (Resource curr_res: resourceList){
            if (in_res.getChannel().equals(curr_res.getChannel()) && 
                    in_res.getOwner().equals(curr_res.getOwner()) &&
                    compareTags(in_res.getTags(), curr_res.getTags()) &&
                    compareUri(in_res.getUri(), curr_res.getUri()) &&
                        (curr_res.getName().contains(in_res.getName()) ||
                        curr_res.getDescription().contains(in_res.getDescription()))) 
            {
                
                //Copy current resource into results list if it matches criterion
                Resource tempRes = new Resource(curr_res.getName(), curr_res.getDescription(), curr_res.getTags(), 
                        curr_res.getUri(), curr_res.getChannel(), 
                        curr_res.getOwner().equals("")? "":"*", curr_res.getEZserver());  //owner is never revealed

                //Send found resource as JSON to client
                results.add(tempRes);
            }
        }
        return results;
    }
    
    //Compare the two URIs for Query purposes. If the incoming URI has no host i.e. is empty, it should match all URIs
    //...otherwise only an exact match is acceptable
    private static boolean compareUri(URI in_uri, URI curr_uri) {
        if (in_uri.getHost() == null) {
            return true;
        } else {
            return in_uri.equals(curr_uri);
        }
    }

    //Compare the two tag sets. The incoming resource template's tags should be a subset of the current resource's.
    private static boolean compareTags(List<String> in_res, List<String> curr_res) {
        if (in_res.size() == 0 && curr_res.size() == 0) {
            return true;
        }
        else if (in_res.size() == 0 && curr_res.size() != 0){
            return false;
        }
        else if (in_res.size() != 0 && curr_res.size() == 0){
            return false;
        }
        else {
            for (String tag: in_res){
                if (!curr_res.contains(tag)) {
                    return false;
                }
            }
        }
        return true;
    }
}