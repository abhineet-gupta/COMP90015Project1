/*
 * Distributed Systems
 * Group Project 1
 * Sem 1, 2017
 * Group: AALT
 *
 * Client-side application
 */

package EZShare;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.sql.Timestamp;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

class Client {
    private static String ip = "localhost";
    private static int port = 3780;
    private static Boolean debug = false; //verbose output   
    private static final int TIMEOUT_SECS = 3;
    private static Boolean relay = false; //For disabling relay to other servers when querying
    private static int CHUNK_SIZE = 1024*1024;
    
    private static final Map<String, Boolean> argOptions;
    static{
        argOptions = new HashMap<>();
        argOptions.put("channel", true);
        argOptions.put("debug", false);
        argOptions.put("description", true);
        argOptions.put("exchange", false);
        argOptions.put("fetch", false);
        argOptions.put("host", true);
        argOptions.put("name", true);
        argOptions.put("owner", true);
        argOptions.put("port", true);
        argOptions.put("publish", false);
        argOptions.put("query", false);
        argOptions.put("remove", false);
        argOptions.put("secret", true);
        argOptions.put("servers", true);
        argOptions.put("share", false);
        argOptions.put("tags", true);
        argOptions.put("uri", true);
        
        //for testing a misbehaving client
        argOptions.put("invalidComm", false);
        argOptions.put("missingComm", false);
    }
    
    public static void main(String[] args) {
        System.out.println("\n" + new Timestamp(System.currentTimeMillis()) + " - [INFO] - Starting the EZShare Client\n"); 

        //possible input arguments
        Options initOptions = new Options();
        for (String option: argOptions.keySet()){
            initOptions.addOption(option, argOptions.get(option), option);
        }
        CommandLineParser parser = new DefaultParser();
        CommandLine initCmd = null;
        
        //Parse provided input arguments
        try {
            initCmd = parser.parse(initOptions, args);
        } catch (ParseException e) {
            System.out.println("Argument parsing error.");
            PrintValidArgumentList();
            System.exit(0);
        }
        
        //Port and IP (aka "host") should be provided
        if (initCmd.hasOption("port")) {
            port = Integer.parseInt(initCmd.getOptionValue("port"));   
        }
        if (initCmd.hasOption("host")) {
            ip = initCmd.getOptionValue("host");
        }
        
        debug = initCmd.hasOption("debug");
        relay = !initCmd.hasOption("norelay");
        
        //Decipher command and call respective method
        if (initCmd.hasOption("publish")) {
            System.out.println(new Timestamp(System.currentTimeMillis()) 
                    + " - [FINE] - publishing to " + ip + ":" + port);
            Client.PublishCmd(initCmd);
        } else if (initCmd.hasOption("remove")) {
            System.out.println(new Timestamp(System.currentTimeMillis()) 
                    + " - [FINE] - removing from " + ip + ":" + port);
            Client.RemoveCmd(initCmd);
        } else if (initCmd.hasOption("share")) {
            System.out.println(new Timestamp(System.currentTimeMillis()) 
                    + " - [FINE] - sharing at " + ip + ":" + port);
            Client.ShareCmd(initCmd);
        } else if (initCmd.hasOption("query")) {
            System.out.println(new Timestamp(System.currentTimeMillis()) 
                    + " - [FINE] - querying " + ip + ":" + port);
            Client.QueryCmd(initCmd);
        } else if (initCmd.hasOption("fetch")) {
            System.out.println(new Timestamp(System.currentTimeMillis()) 
                    + " - [FINE] - fetching from " + ip + ":" + port);
            Client.FetchCmd(initCmd);
        } else if (initCmd.hasOption("exchange") && initCmd.hasOption("servers")) {
            System.out.println(new Timestamp(System.currentTimeMillis()) 
                    + " - [FINE] - exchanging with " + ip + ":" + port);
            Client.ExchangeCmd(initCmd);
        } else if (initCmd.hasOption("invalidComm")) {
            Client.InvalidCmd();
        } else if (initCmd.hasOption("missingComm")) {
            Client.MissingCmd();
        } else {
            System.out.println("Command not supplied.");
            PrintValidArgumentList();;
        }
    }    

    @SuppressWarnings("unchecked")  
    public static void PublishCmd(CommandLine initCmd) {
        JSONObject command = new JSONObject();
        
        //Create JSON object from resource info provided in CMD
        JSONObject resource = createResJSONObj(initCmd);

        command.put("command", "PUBLISH");
        command.put("resource", resource);
        
        //send off to server
        generalReply(command.toJSONString());
    }
    
    @SuppressWarnings("unchecked")
    public static void RemoveCmd(CommandLine initCmd) {
        JSONObject command = new JSONObject();
        JSONObject resource = createResJSONObj(initCmd);

        command.put("command", "REMOVE");
        command.put("resource", resource);
        generalReply(command.toJSONString());
    }
    
    @SuppressWarnings("unchecked")
    public static void ShareCmd(CommandLine initCmd) {
        JSONObject command = new JSONObject();
        JSONObject resource = createResJSONObj(initCmd);

        //Sharing a file on server requires secret value
        String secret = initCmd.getOptionValue("secret");
        
        command.put("command", "SHARE");
        command.put("secret", secret);
        command.put("resource", resource);
        generalReply(command.toJSONString());
    }
    
    @SuppressWarnings("unchecked")
    private static void QueryCmd(CommandLine initCmd) {
        JSONObject command = new JSONObject();
        JSONObject resourceTemplate = createResJSONObj(initCmd);
        
        command.put("command", "QUERY");
        command.put("relay", relay);
        command.put("resourceTemplate", resourceTemplate);
        generalReply(command.toJSONString());
    }
    
    @SuppressWarnings("unchecked")
    public static void FetchCmd(CommandLine initCmd) {
    	JSONObject command = new JSONObject();
    	JSONObject resourceTemplate = createResJSONObj(initCmd);

        command.put("command", "FETCH");
        command.put("resourceTemplate", resourceTemplate);
        
        try (Socket socket = new Socket(ip, port)){
            //Get I/O streams for connection
            DataInputStream input = new DataInputStream(socket.getInputStream());
            DataOutputStream output = new DataOutputStream(socket.getOutputStream());
            
            //record start time
            long start = System.currentTimeMillis();
            
            //send request
            output.writeUTF(command.toJSONString());
            if (debug) {
                System.out.println(new Timestamp(System.currentTimeMillis())
                        +" - [DEBUG] - SENT: " + command);
            }
            output.flush();
            
            JSONParser JSONparser = new JSONParser();
            
            while(true) {
            	if(input.available() > 0) {
            		String result = input.readUTF();
            		if (debug) {
                        System.out.println(new Timestamp(System.currentTimeMillis())
                                +" - [DEBUG] - RECEIVED: " + result);
                    }
            		JSONObject reply = (JSONObject) JSONparser.parse(result);
            		//the first reply: response
            		if(reply.containsKey("response")) {
            			if(reply.get("response").equals("success")) {
            				System.out.println("Success");
            			    continue;
            			} else {
            				System.out.println("Error occurred.");
            				break;
            			}
            		}
            		//the second reply: resource and file
            		if(reply.containsKey("resourceSize")) {
            			Long fileSizeRemaining = (Long) reply.get("resourceSize");            			
            			int chunkSize = setChunkSize(fileSizeRemaining);            			
            			byte[] receiveBuffer = new byte[chunkSize];
            			String uri = (String)reply.get("uri");
            			
            			//this only works for Linux?
            			String fileName = uri.substring(uri.lastIndexOf('/') + 1, uri.length());
            			RandomAccessFile downloadingFile = new RandomAccessFile(fileName, "rw");

            			//remaining file size
            			int num;
                        System.out.println(new Timestamp(System.currentTimeMillis())
                                    +" - [INFO] - Downloading file of size: " 
                                    + fileSizeRemaining + " bytes.");
                        
                        //read actual file
            			while((num = input.read(receiveBuffer)) > 0) {
            				downloadingFile.write(Arrays.copyOf(receiveBuffer, num));
            				fileSizeRemaining -= num;
            				
            				chunkSize = setChunkSize(fileSizeRemaining);
            				receiveBuffer = new byte[chunkSize];
            				
            				if(fileSizeRemaining == 0){
            					break;
            				}
            			}
            			
            			System.out.println(new Timestamp(System.currentTimeMillis())
            			        +" - [INFO] - File downloaded.");
            			downloadingFile.close();
            		}
            		
            		//the last reply: resultSize
            		if(reply.containsKey("resultSize")) {
            		    break;
            		}
            	}
            	//connection timeout
                if((System.currentTimeMillis() - start) > TIMEOUT_SECS*1000) {
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    @SuppressWarnings("unchecked")
    private static void ExchangeCmd(CommandLine initCmd) {
        JSONObject command = new JSONObject();
        String[] serversArr = initCmd.getOptionValue("servers").split(",");
        JSONArray servers = new JSONArray();
        
        //parse servers in list and add to JSON array
        for(String server : serversArr) {
            String[] hostAndPort = server.split(":");
            JSONObject ele = new JSONObject();
            ele.put("hostname", hostAndPort[0]);
            ele.put("port", Integer.parseInt(hostAndPort[1]));
            servers.add(ele);
        }
        command.put("command", "EXCHANGE");
        command.put("serverList", servers);
        generalReply(command.toJSONString());
    }
    
    public static int setChunkSize(long fileSizeRemaining) {
        if(fileSizeRemaining < CHUNK_SIZE) {
            CHUNK_SIZE = (int) fileSizeRemaining;
        }
        return CHUNK_SIZE;
    }
    
    @SuppressWarnings("unchecked")
    private static void MissingCmd() {
        JSONObject jobj = new JSONObject();
        jobj.put("NoCommand", "blah");
        Client.generalReply(jobj.toString());
    }
        
    @SuppressWarnings("unchecked")
    private static void InvalidCmd() {
        JSONObject jobj = new JSONObject();
        jobj.put("command", "invalidCommand");
        Client.generalReply(jobj.toString());   
    }
    
    //Create a JSON object from resource information given in CMD arguments 
    @SuppressWarnings("unchecked")  
    private static JSONObject createResJSONObj(CommandLine initCmd) {
        JSONObject resource = new JSONObject();
        
        //replace non-existent values with blank string
        String uri = initCmd.hasOption("uri")? initCmd.getOptionValue("uri") : "";
        String name = initCmd.hasOption("name") ? initCmd.getOptionValue("name") : "";
        
        //parse tags into JSON Array
        JSONArray tag_list = new JSONArray();
        if (initCmd.hasOption("tags")) {
            String[] tags_arr = initCmd.getOptionValue("tags").split(",");
            tag_list = new JSONArray();
            for (String tag: tags_arr){
                tag_list.add(tag);
            }
        }
        
        String description = initCmd.hasOption("description") ? 
                initCmd.getOptionValue("description") : "";
        String channel = initCmd.hasOption("channel") ? 
                initCmd.getOptionValue("channel") : "";
        String owner = initCmd.hasOption("owner") ? 
                initCmd.getOptionValue("owner") : "";
                
        resource.put("name", name);            
        resource.put("tags", tag_list);
        resource.put("description", description);
        resource.put("uri", uri);
        resource.put("channel", channel);
        resource.put("owner", owner);
        resource.put("ezserver", null);
        
        return resource;
    }
    
    //Send JSON command to server
    public static void generalReply(String request) {
        try (Socket socket = new Socket(ip, port)){
            //Get I/O streams for connection
            DataInputStream input = new DataInputStream(socket.getInputStream());
            DataOutputStream output = new DataOutputStream(socket.getOutputStream());

            //record start time
            long startTime = System.currentTimeMillis();
            
            //send request
            output.writeUTF(request);
            output.flush();
            if (debug) {
                System.out.println(new Timestamp(System.currentTimeMillis())
                        + " - [DEBUG] - SENT: " + request);
            }
                        
            JSONParser parser = new JSONParser();
            while(true) {
                if(input.available() > 0) {
                    String recv = input.readUTF();
                    JSONObject reply = (JSONObject) parser.parse(recv);
                    if (debug) {
                        System.out.println(new Timestamp(System.currentTimeMillis())
                                + " - [DEBUG] - RECEIVED: " + recv);
                    }
                    else {
                        System.out.println("Response from server: " + recv);
                    }
                    if (reply.containsKey("resultSize")) {
                        break;
                    }
                }
                if ((System.currentTimeMillis() - startTime) > TIMEOUT_SECS*1000){
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static void PrintValidArgumentList() {
        System.out.println("Valid arguments include: \n"
                + "\t -channel <arg>    channel \n"
                + "\t -debug            print debug information \n"
                + "\t -description <arg> resource description \n"
                + "\t -exchange         exchange server list with server \n"
                + "\t -fetch            fetch resources from server \n"
                + "\t -host <arg>       server host, a domain name or IP address \n"
                + "\t -name <arg>       resource name \n"
                + "\t -owner <arg>      owner \n"
                + "\t -port <arg>       server port, an integer \n"
                + "\t -publish          publish resource on server \n"
                + "\t -query            query for resources from server \n"
                + "\t -norelay          do not relay query command to other servers \n"
                + "\t -remove           remove resource from server \n"
                + "\t -secret <arg>     secret \n"
                + "\t -servers <arg>    server list, host1:port1,host2:port2,... \n"
                + "\t -share            share resource on server \n"
                + "\t -tags <arg>       resource tags, tag1,tag2,tag3,... \n"
                + "\t -uri <arg>        resource URI \n");
    }
}