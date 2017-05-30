/*
 * Distributed Systems
 * Group Project 2
 * Sem 1, 2017
 * Group: AALT
 * 
 * For each relay connection for each client, creates a new thread
 * # of threads = # of servers x # of clients subscriptions[w/ relay]
 */


package EZShare;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.sql.Timestamp;
import java.util.List;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

class ListenRelay extends Thread{
    private Socket server;
    private DataOutputStream sendToClient;
    private volatile boolean newTemplateFlag = false;
    private volatile boolean unsubscribeFlag = false;
    private List<Resource> template;
    private Resource newTemplate;
    private String id;
    private Boolean debug = false;
    private int resultCount = 0;
    
   
    		
    public ListenRelay(DataOutputStream sendToClient, Socket server, 
            List<Resource> template, String id, Boolean debug) {
        this.sendToClient = sendToClient;
        this.server = server;
        this.template = template;
        this.id = id;
        this.debug = debug;
    }

    public void setNewTemplateFlag(Resource newTemplate) {
        this.newTemplate = newTemplate;
        this.newTemplateFlag = true;
    }
    
    public void setUnsubscribeFlag() {
        this.unsubscribeFlag = true;
    }
    
    public int getResultSize() {
    	return this.resultCount;
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public void run() {
        DataInputStream serverReply;
        DataOutputStream sendToServer;
        
        try {
            serverReply = new DataInputStream(server.getInputStream());
            sendToServer = new DataOutputStream(server.getOutputStream());
            JSONObject command;
            JSONObject resourceTemplate;
            JSONParser parser = new JSONParser();
            
            for(Resource res : template) {
                //send the first subscribe command here
                command = new JSONObject();
                resourceTemplate = Connection.Resource2JSONObject(res);

                command.put("command", "SUBSCRIBE");
                command.put("relay", false);
                command.put("id",this.id);
                command.put("resourceTemplate", resourceTemplate);

                sendToServer.writeUTF(command.toJSONString());
                if (debug) {
                    System.out.println(new Timestamp(System.currentTimeMillis())
                            + " - [DEBUG] - SENT: " + command.toJSONString());
                }
            }
            String result;
            while(true) {
                if((result = serverReply.readUTF()) != null) {
                	JSONObject resultJSON = (JSONObject) parser.parse(result);
                	
                    if (debug) {
                        System.out.println(new Timestamp(System.currentTimeMillis())
                                + " - [DEBUG] - RECEIVED: " + result);
                    }
                    
                    if(!resultJSON.containsKey("response") && !resultJSON.containsKey("resultSize")) {
                        sendToClient.writeUTF(result);
                        if (debug) {
                            System.out.println(new Timestamp(System.currentTimeMillis())
                                    + " - [DEBUG] - SENT: " + result);
                        }
                    }
                    
                    if(resultJSON.containsKey("resultSize")) {
                    	this.resultCount = Integer.parseInt(resultJSON.get("resultSize").toString());
                    	break;
                    }
                    
                }
                
                if(newTemplateFlag) {
                    //send relay subscribe command here
                    command = new JSONObject();
                    resourceTemplate = Connection.Resource2JSONObject(this.newTemplate);
                    
                    command.put("command", "SUBSCRIBE");
                    command.put("relay", false);
                    command.put("id",this.id);
                    resourceTemplate.put("channel", "");
                    resourceTemplate.put("owner", "");
                    command.put("resourceTemplate", resourceTemplate);                 
                    sendToServer.writeUTF(command.toJSONString());
                    
					if (debug) {
						System.out.println(new Timestamp(System.currentTimeMillis())
								+ " - [DEBUG] - RECEIVED: " + command.toJSONString());
					}	
                }
                if(this.unsubscribeFlag) {
                    break;
                }
            }
        } catch (SocketException e){
            if (debug) {
                System.out.println(new Timestamp(System.currentTimeMillis())
                        +" - [FINE] - (ListenRelay) Connection closed by server.");
            }
        } catch (SocketTimeoutException e) {
            if (debug) {
                System.out.println(new Timestamp(System.currentTimeMillis())
                        +" - [FINE] - (ListenRelay) Connection closed.");
            }
        } catch (EOFException e){
            if (debug){
                System.out.println(new Timestamp(System.currentTimeMillis())
                        +" - [FINE] - (ListenRelay) Connection closed by server.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
}