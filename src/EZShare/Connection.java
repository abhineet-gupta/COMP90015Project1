/*
 * Distributed Systems
 * Group Project 1
 * Sem 1, 2017
 * Group: AALT
 * 
 * Class that handles connections with each client
 * It runs as a separate thread
 * It implements the major functionality expected of the server e.g. Publish, Remove, etc.
 */

package EZShare;

import java.io.*;
import java.net.*;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.json.simple.JSONArray;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Connection implements Runnable {
	private static final int SECS_TO_TIMEOUT = 2;  //how long to wait for an open connection
    private Socket client;
	private String serverSecret;
	private Boolean debug;
	private String hostname;
	private int port;
	
	public Connection(CommandLine cmd, Socket client, String secret, String hostname, int port) {
		this.client = client;
		this.serverSecret = secret;
		this.hostname = hostname;
		this.debug = cmd.hasOption("debug") ? true : false;
		this.port = port;
	}
	
	@SuppressWarnings("unchecked")
	//JSONObject extends HashMap but does not have type parameters as HashMap would expect...
	
    @Override
	public void run() {
		try {
			DataInputStream input = new DataInputStream(client.getInputStream());
			DataOutputStream output = new DataOutputStream(client.getOutputStream());			
			JSONParser parser = new JSONParser();
			
			JSONObject client_request = (JSONObject) parser.parse(input.readUTF());
			if (debug) {
                System.out.println(new Timestamp(System.currentTimeMillis())
                        + " - [DEBUG] - RECEIVED: " + client_request);
            }
			
			if (!client_request.containsKey("command")) throw new ClassCastException();
			
			switch ((String) client_request.get("command")) {
			case "PUBLISH":
				publish(client_request, output);
				break;
			case "REMOVE":
				remove(client_request, output);
				break;
			case "SHARE":
                share(client_request, output);
                break;
			case "QUERY":
                query(client_request, output);
                break;
			case "FETCH":
				fetch(client_request, output);
				break;
			case "EXCHANGE":
                exchange(client_request, output);
                break;
			default:
				JSONObject reply = new JSONObject();
				reply.put("response", "error");
				reply.put("errorMessage", "invalid command");
				output.writeUTF(reply.toJSONString());
				if (debug) {
                    System.out.println(new Timestamp(System.currentTimeMillis())
                            + " - [DEBUG] - SENT: " + reply.toJSONString());
                }
			}	
		} catch (IOException e) {
		    e.printStackTrace();
		} catch (ParseException | ClassCastException e) {
			JSONObject reply = new JSONObject();
			reply.put("response", "error");
			reply.put("errorMessage", "missing or incorrect type for command");
			try {
				DataOutputStream output = new DataOutputStream(client.getOutputStream());
				output.writeUTF(reply.toJSONString());
				if (debug) {
                    System.out.println(new Timestamp(System.currentTimeMillis()) 
                            + " - [DEBUG] - SENT: " + reply.toJSONString());
                }
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}		
	}
	
	@SuppressWarnings("unchecked")
    private void publish(JSONObject client_req, DataOutputStream output) throws IOException {
        JSONParser parser = new JSONParser();
        try{
            //throws class cast exception for missing resource
            JSONObject resourceJSON = (JSONObject) 
                    parser.parse(client_req.get("resource").toString());
            
            //throws server exception invalid resource
            Resource resource = JSONObj2Resource(resourceJSON);
            
            URI uri = resource.getUri();
            //check if URI is provided
            if (uri.toString().equals("")) throw new serverException("invalid resource");
            
            //check if URI is a URL
            boolean isWeb = "http".equalsIgnoreCase(uri.getScheme())
                    || "https".equalsIgnoreCase(uri.getScheme())
                    || "ftp".equalsIgnoreCase(uri.getScheme());
            if (!isWeb) throw new serverException("invalid resource");
            
            //throws cannot publish resource if any publishing rules are broken
            ResourceList.addResource(resource);
            
            //create a reply
            JSONObject reply = new JSONObject();
            reply.put("response", "success");
            output.writeUTF(reply.toJSONString());
            if (debug) {
                System.out.println(new Timestamp(System.currentTimeMillis())
                        + " - [DEBUG] - SENT: " + reply.toJSONString());
            }
        } catch (ParseException | ClassCastException e) {
            JSONObject reply = new JSONObject();
            reply.put("response", "error");
            reply.put("errorMessage", "missing resource");
            output.writeUTF(reply.toJSONString());
            if (debug) {
                System.out.println(new Timestamp(System.currentTimeMillis())
                        + " - [DEBUG] - SENT: " + reply.toJSONString());
            }
        } catch(serverException e) {
            JSONObject reply = new JSONObject();
            reply.put("response", "error");
            reply.put("errorMessage", e.toString());
            output.writeUTF(reply.toJSONString());
            if (debug) {
                System.out.println(new Timestamp(System.currentTimeMillis())
                        + " - [DEBUG] - SENT: " + reply.toJSONString());
            }
        }
    }
	
	@SuppressWarnings("unchecked")
    private void remove(JSONObject client_req, DataOutputStream output) throws IOException {
        JSONParser parser = new JSONParser();
        try {
            //throw class cast exception for missing resource
            JSONObject resourceJSON = (JSONObject) 
                    parser.parse(client_req.get("resource").toString());
            
            //throw server exception invalid resource
            Resource resource = JSONObj2Resource(resourceJSON);
            
            //throw server exception cannot remove resource
            ResourceList.removeResource(resource);
            
            //create a reply
            JSONObject reply = new JSONObject();
            reply.put("response", "success");
            output.writeUTF(reply.toJSONString());
            if (debug) {
                System.out.println(new Timestamp(System.currentTimeMillis())
                        + " - [DEBUG] - SENT: " + reply.toJSONString());
            }
            
        } catch (ParseException | ClassCastException e) {
            JSONObject reply = new JSONObject();
            reply.put("response", "error");
            reply.put("errorMessage", "missing resource");
            output.writeUTF(reply.toJSONString());
            if (debug) {
                System.out.println(new Timestamp(System.currentTimeMillis())
                        + " - [DEBUG] - SENT: " + reply.toJSONString());
            }
            
        } catch(serverException e) {
            JSONObject reply = new JSONObject();
            reply.put("response", "error");
            reply.put("errorMessage", e.toString());
            output.writeUTF(reply.toJSONString());
            if (debug) {
                System.out.println(new Timestamp(System.currentTimeMillis())
                        + " - [DEBUG] - SENT: " + reply.toJSONString());
            }
        }
    }
	
	@SuppressWarnings("unchecked")
    private void share(JSONObject client_req, DataOutputStream output) throws IOException {
        JSONParser parser = new JSONParser();
        try {
            //throw class cast exception for missing resource and\/or secret
            JSONObject resourceJSON = (JSONObject) 
                    parser.parse(client_req.get("resource").toString());
            if(!client_req.containsKey("secret")) throw new ClassCastException();
            
            //throw server exception invalid resource
            Resource resource = JSONObj2Resource(resourceJSON);
            
            URI uri = resource.getUri();
            //check if URI is provided and is absolute
            if (uri.toString().equals("")) throw new serverException("invalid resource");
            
            //check if URI is a file scheme
            boolean isFile = "file".equalsIgnoreCase(uri.getScheme());
            if (!isFile) throw new serverException("invalid resource");
            
            //check if the file exist
            File f = new File(uri.getPath());
            if (!f.exists()) throw new serverException("invalid resource");
            
            //check if secret is equal
            if (!client_req.get("secret").equals(this.serverSecret)) 
                throw new serverException("incorrect secret");
            
            //throw cannot publish resource
            ResourceList.addResource(resource);
            
            //create a reply
            JSONObject reply = new JSONObject();
            reply.put("response", "success");
            output.writeUTF(reply.toJSONString());
            if (debug) {
                System.out.println(new Timestamp(System.currentTimeMillis())
                        + " - [DEBUG] - SENT: " + reply.toJSONString());
            }
        } catch (ParseException | ClassCastException e) {
            JSONObject reply = new JSONObject();
            reply.put("response", "error");
            reply.put("errorMessage", "missing resource and/or secret");
            output.writeUTF(reply.toJSONString());
            if (debug) {
                System.out.println(new Timestamp(System.currentTimeMillis())
                        + " - [DEBUG] - SENT: " + reply.toJSONString());
            }
        } catch (serverException e) {
            JSONObject reply = new JSONObject();
            reply.put("response", "error");
            if(e.toString().equals("cannot publish resource")) {
                reply.put("errorMessage", "cannot share resource");
            } else {
                reply.put("errorMessage", e.toString());
            }
            output.writeUTF(reply.toJSONString());
            if (debug) {
                System.out.println(new Timestamp(System.currentTimeMillis())
                        + " - [DEBUG] - SENT: " + reply.toJSONString());
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void query(JSONObject client_request, DataOutputStream output) throws IOException {                

    	if (client_request.containsKey("resourceTemplate")) {
            try {
                Resource in_res;
                JSONParser parser = new JSONParser();
                
                // The following should throw an invalid resource exception
                in_res = this.JSONObj2Resource((JSONObject) 
                        parser.parse(client_request.get("resourceTemplate").toString()));
                
                // If there was a resourceTemplate AND the template checked out as valid
                JSONObject response = new JSONObject();
                response.put("response", "success");
                output.writeUTF(response.toJSONString());
                if (debug) {
                    System.out.println(new Timestamp(System.currentTimeMillis())
                            + " - [DEBUG] - SENT: " + response.toJSONString());
                }
                
                //get results from other servers first if relay == true
                int result_cnt = 0;
                List<Resource> relay_res_results = new ArrayList<Resource>();
                if (client_request.containsKey("relay")) {
                    //only propagate when there are other servers in the list
                    if ((Boolean)client_request.get("relay") && ServerList.getLength() > 0) {
                        relay_res_results = propagateQuery(client_request);
                        result_cnt += relay_res_results.size();
                    }
                }

                //store local query results here
                List<Resource> res_results = new ArrayList<Resource>();                
                res_results = ResourceList.queryForQuerying(in_res);
                result_cnt += res_results.size();
                
                //combine the two results
                for (Resource res: relay_res_results){
                    res_results.add(res);
                }

                //send each resource to client
                for (Resource res: res_results){
                    JSONObject resource_temp = this.Resource2JSONObject(res); 
                    output.writeUTF(resource_temp.toJSONString());
                    if (debug) {
                        System.out.println(new Timestamp(System.currentTimeMillis())
                                + " - [DEBUG] - SENT: " + resource_temp.toJSONString());
                    }
                }
                
                //Send number of results found to server
                JSONObject result_size = new JSONObject();
                result_size.put("resultSize", result_cnt);
                output.writeUTF(result_size.toJSONString());
                if (debug) {
                    System.out.println(new Timestamp(System.currentTimeMillis())
                            + " - [DEBUG] - SENT: " + result_size.toJSONString());
                }
                
            } catch (Exception e) { //invalid resourceTemplate
                JSONObject inv_res = new JSONObject();
                inv_res.put("response", "error");
                inv_res.put("errorMessage", "invalid resourceTemplate");
                output.writeUTF(inv_res.toJSONString());
                if (debug) {
                    System.out.println(new Timestamp(System.currentTimeMillis())
                            + " - [DEBUG] - SENT: " + inv_res.toJSONString());
                }
            }            
        } else {
            // Missing resource template error - send to client
            JSONObject error = new JSONObject();
            error.put("response", "error");
            error.put("errorMessage", "missing resourceTemplate");
            output.writeUTF(error.toJSONString());
            if (debug) {
                System.out.println(new Timestamp(System.currentTimeMillis())
                        + " - [DEBUG] - SENT: " + error.toJSONString());
            }
        }      
    }

    @SuppressWarnings("unchecked")
    private List<Resource> propagateQuery(JSONObject client_request) 
            throws ParseException, UnknownHostException, IOException, serverException {
        List<Resource> prop_results = new ArrayList<>();
        
        //construct the right query
        JSONParser parser = new JSONParser();
        JSONObject res = (JSONObject) 
                parser.parse(client_request.get("resourceTemplate").toString());
        JSONObject command = new JSONObject();
        
        //remove owner and channel and set to "" + relay = false
        command.put("command", "QUERY");
        command.put("relay", false);
            res.put("owner", "");
            res.put("channel", "");
        command.put("resourceTemplate", res);

        //for each server from server list
        JSONArray serv_list = ServerList.getCopyServerList();
        for (int i = 0; i < serv_list.size(); i++) {

            //Get server details
            JSONObject server = (JSONObject)serv_list.get(i);
            String hostname = (String) server.get("hostname");
            int port = Integer.parseInt(server.get("port").toString());            
            
            //Send QUERY command to that server
            try {
                Socket socket = new Socket(hostname, port);
                socket.setSoTimeout(SECS_TO_TIMEOUT);
                //Get I/O streams for connection
                DataInputStream input = new DataInputStream(socket.getInputStream());
                DataOutputStream output = new DataOutputStream(socket.getOutputStream());
                
                //record start time
                long startTime = System.currentTimeMillis();
                
                //send request
                output.writeUTF(command.toJSONString());
                if (debug) {
                    System.out.println(new Timestamp(System.currentTimeMillis())
                            + " - [DEBUG] - SENT: " + command.toJSONString());
                }
                output.flush();
                            
                while (true) {
                    if (input.available() > 0) {
                        //get results and store in results list
                        JSONObject temp_response = (JSONObject) parser.parse(input.readUTF());
                        if (debug) {
                            System.out.println(new Timestamp(System.currentTimeMillis())
                                    + " - [DEBUG] - RECEIVED: " + temp_response);
                        }
                        if (temp_response.containsKey("uri")) {
                            Resource temp_res = this.JSONObj2Resource(temp_response);
                            prop_results.add(temp_res);
                        }
                    }
                    if ((System.currentTimeMillis() - startTime) > SECS_TO_TIMEOUT*1000){
                        break;
                    }
                }
                socket.close();
            } catch (ConnectException e) {
                //If connection times out for a particular server, just move on
                //...to the next one in the list. 
                //The Exchange process is responsible for removing dud ones.
            }
        }               
        return prop_results;
    }
	
	@SuppressWarnings("unchecked")
    private void fetch(JSONObject client_req, DataOutputStream output) throws IOException{
		JSONParser parser = new JSONParser();
		try{
			//throw class cast exception for missing resource
			JSONObject resourceJSON = (JSONObject) 
			        parser.parse(client_req.get("resourceTemplate").toString());
			
			//throw server exception invalid resource
			Resource resourceTemplate = JSONObj2Resource(resourceJSON);
		
			Resource match = ResourceList.queryForChannelURI(resourceTemplate);
		
			//handle no match error
			if(match == null) throw new serverException("no match resource");
		
			//Open and read file for sending 
			URI uri = match.getUri();
			File f = new File(uri.getPath());
			if(f.exists()) {
				JSONObject response = new JSONObject();
				response.put("response", "success");
				output.writeUTF(response.toJSONString());
				if (debug) {
	                System.out.println(new Timestamp(System.currentTimeMillis())
	                        + " - [DEBUG] - SENT: " + response.toJSONString());
	            }
			
				//Because it's read in 'read' mode, it is thread-safe
				RandomAccessFile byteFile = new RandomAccessFile(f, "r");
				JSONObject resource = Resource2JSONObject(match);
				resource.put("owner", "*"); //protect privacy of owner
				
				//send file size
				resource.put("resourceSize", byteFile.length());
				output.writeUTF(resource.toJSONString());
				if (debug) {
	                System.out.println(new Timestamp(System.currentTimeMillis())
	                        + " - [DEBUG] - SENT: " + resource.toJSONString());
	            }
			
				//send file
				byte[] sendingBuffer = new byte[1024*1024];
				int num;
				while((num = byteFile.read(sendingBuffer)) > 0) {
					output.write(Arrays.copyOf(sendingBuffer, num));
				}
				byteFile.close();
			
				//Send resultSize as 1 to indicate completion
				JSONObject resultFile = new JSONObject();
				resultFile.put("resultSize", 1);
				output.writeUTF(resultFile.toJSONString());
				if (debug) {
	                System.out.println(new Timestamp(System.currentTimeMillis())
	                        + " - [DEBUG] - SENT: " + resultFile.toJSONString());
	            }
			}
			
		} catch (ParseException | ClassCastException e) {
			JSONObject reply = new JSONObject();
			reply.put("response", "error");
			reply.put("errorMessage", "missing resourceTemplate");
			output.writeUTF(reply.toJSONString());
			if (debug) {
                System.out.println(new Timestamp(System.currentTimeMillis())
                        + " - [DEBUG] - SENT: " + reply.toJSONString());
            }
			
		} catch (serverException e) {
			JSONObject reply = new JSONObject();
			reply.put("response", "error");
			if(e.toString().equals("invalid resource")) {
				reply.put("errorMessage", "invalid resourceTemplate");
			} else {
				reply.put("errorMessage", e.toString());
			}
			output.writeUTF(reply.toJSONString());
			if (debug) {
                System.out.println(new Timestamp(System.currentTimeMillis()) 
                        + " - [DEBUG] - SENT: " + reply.toJSONString());
            }
		} 
	}

    @SuppressWarnings("unchecked")
    private void exchange(JSONObject client_request, DataOutputStream output) 
            throws IOException {
        JSONArray newServerList = new JSONArray();
        try{
            JSONParser parser = new JSONParser();
            //parse new server list
            newServerList = (JSONArray) 
                    parser.parse(client_request.get("serverList").toString());
        } catch (ClassCastException | ParseException e) {
            JSONObject reply = new JSONObject();
            reply.put("response", "error");
            reply.put("errorMessage", "missing or invalid server list");
            output.writeUTF(reply.toJSONString());
            if (debug) {
                System.out.println(new Timestamp(System.currentTimeMillis())
                        + " - [DEBUG] - SENT: " + reply.toJSONString());
            }
        }
        try {
            //try to union the new and current list of servers
            ServerList.update(newServerList, hostname, port);
            
            JSONObject reply = new JSONObject();
            reply.put("response", "success");
            output.writeUTF(reply.toJSONString());
            if (debug) {
                System.out.println(new Timestamp(System.currentTimeMillis())
                        + " - [DEBUG] - SENT: " + reply.toJSONString());
            }
            
        } catch (NumberFormatException | ClassCastException | 
                UnknownHostException | serverException e) {
            JSONObject reply = new JSONObject();
            reply.put("response", "error");
            reply.put("errorMessage", "invaild server record");
            output.writeUTF(reply.toJSONString());
            if (debug) {
                System.out.println(new Timestamp(System.currentTimeMillis())
                        + " - [DEBUG] - SENT: " + reply.toJSONString());
            }
        }
    }
	
    //Convert incoming resource (in JSON format) into a Resource object
	private Resource JSONObj2Resource(JSONObject resource) throws serverException {
		//handle 3 potential cases for most fields:
	    //...1) Incoming resource does not contain a field; replace with ""
	    //...2) It contains the field, but it's null; replace with ""
	    //...3) It contains the field AND the value - use value as is
	    
		String Name = resource.containsKey("name") ? 
		        (resource.get("name") == null ? 
		                "" : (String) resource.get("name")) 
		        : "";
		        
		if (Name.contains("\\0")) {
			throw new serverException("Invalid resource");
		}
		String Description = resource.containsKey("description") ? 
		        (resource.get("description") == null ? 
		                "" : (String) resource.get("description")) 
		        : "";
		        
		if (Description.contains("\\0")) {
			throw new serverException("Invalid resource");
		}
		
		JSONArray tags_jarr = resource.containsKey("tags") ? 
		        (JSONArray) resource.get("tags") : null;
		List<String> tags_slist = new ArrayList<String>();
		if (tags_jarr.size() != 0) {
		    for (Object tag: tags_jarr){
		        tags_slist.add((String) tag);
		    }
		}
		
		String uri_s = resource.containsKey("uri") ? 
		        (resource.get("uri") == null ? 
		                "" : resource.get("uri").toString()) 
		        : "";
		if (uri_s.contains("\\0")) {
			throw new serverException("Invalid resource");
		}
		URI uri;
		try {
			uri = new URI(uri_s);
			if (!uri_s.equals("") && !uri.isAbsolute()) throw new serverException("Invalid resource");
		}
		catch (URISyntaxException e) {
			throw new serverException("Invalid resource");
		}
		
		String Channel = resource.containsKey("channel") ? 
		        (resource.get("channel") == null ? 
		                "" : (String) resource.get("channel")) 
		        : "";
		if (Channel.contains("\\0")) {
			throw new serverException("Invalid resource");
		}
		
		String Owner = resource.containsKey("owner") ?
		        (resource.get("owner") == null ?
		                "" : (String) resource.get("owner")) 
		        : "";
		if (Owner.contains("\\0") || Owner.contains("*")) {
			throw new serverException("Invalid resource");
		}
		
		//Set EZserver as current server if empty
		String curr_EZserver = hostname+":"+port; 
		String EZServer = resource.containsKey("ezserver") ? 
		        (resource.get("ezserver") == null ? 
		                curr_EZserver : (String) resource.get("ezserver")) 
		        : curr_EZserver;
        if (EZServer.contains("\\0")) {
            throw new serverException("Invalid resource");
        }
		
		return new Resource(Name, Description, tags_slist, uri, Channel, Owner, EZServer);
	}
	
	//Reverse of the above; convert Resource object to JSON
	@SuppressWarnings("unchecked")
    private JSONObject Resource2JSONObject(Resource resource) {
        JSONObject jobj = new JSONObject();

        jobj.put("name", resource.getName());
        jobj.put("description", resource.getDescription());
        
        JSONArray tag_list = new JSONArray();
        for (String tag: resource.getTags()){
            tag_list.add(tag);
        }
        jobj.put("tags", tag_list);
        
        jobj.put("uri", resource.getUri().toString());
        jobj.put("channel", resource.getChannel());
        jobj.put("owner", resource.getOwner());
        jobj.put("ezserver", resource.getEZserver());
    
        return jobj;
	}
}
