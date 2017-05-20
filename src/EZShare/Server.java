/*
 * Distributed Systems
 * Group Project 1
 * Sem 1, 2017
 * Group: AALT
 * 
 * Server-side application
 */

package EZShare;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.net.ServerSocketFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;


public class Server {

    //minimum time between each successive connection from the same IP address
    private static long connectionIntervalLimit = 1*1000;   //milliseconds
    private static long exchangeIntervalLimit = 10*60;   //seconds
    //max number of concurrent client connections allowed
    private static final int MAX_THREADS = 10;
    //TODO leave the public members as is? Made public for Exchanger to access
    public static final int SOCKET_TIMEOUT_MS = 2*1000;    //ms
    
    private static int connections_cnt = 0;
    private static int port = 3780;
    private static int sPort = 3781; 
    public static String hostname;
    private static String secret;
    public static Boolean debug = false;
    private static final Map<String, Boolean> argOptions;
    private static Map<String, Long> clientIPList = new HashMap<>();
    
    static{
        argOptions = new HashMap<>();
        argOptions.put("advertisedhostname", true);
        argOptions.put("connectionintervallimit", true);
        argOptions.put("exchangeinterval", true);
        argOptions.put("port", true);
        argOptions.put("secret", true);
        argOptions.put("debug", false);
    }
    
    public static void main(String[] args) {        
        //Set truststore and keystore with its password
        System.setProperty("javax.net.ssl.trustStore", "keystores/server.jks");
        System.setProperty("javax.net.ssl.keyStore","keystores/server.jks");
        System.setProperty("javax.net.ssl.keyStorePassword","aalt_s");

        // Enable debugging to view the handshake and communication which happens between the SSLClient and the SSLServer
//        System.setProperty("javax.net.debug","all");
        
        //Parse CMD options
        Options options = new Options();
        for (String option: argOptions.keySet()){
            options.addOption(option, argOptions.get(option), option);
        }
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println("Please provide correct options.");
            PrintValidArgumentList();
            System.exit(0);
        }
        
        if (cmd.hasOption("port")) {
            port = Integer.parseInt(cmd.getOptionValue("port"));
        }
        
        //self assign a hostname
        if (cmd.hasOption("advertisedhostname")) {
            Server.hostname = cmd.getOptionValue("advertisedhostname");
        } else {
            try {
                Server.hostname = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                System.out.println(new Timestamp(System.currentTimeMillis()) 
                        + " - [ERROR] - Could not determine hostname of self."
                        + " Please provide via options.");
                PrintValidArgumentList();
            }
        }
        if (cmd.hasOption("debug")) {
            Server.debug = true;
            System.out.println(new Timestamp(System.currentTimeMillis()) 
                    + " - [INFO] - setting debug on\n");
        }
        if (cmd.hasOption("exchangeinterval")) {
            Server.exchangeIntervalLimit = 
                    Long.parseLong(cmd.getOptionValue("exchangeinterval"));
        }
        if (cmd.hasOption("connectionintervallimit")) {
            Server.connectionIntervalLimit = 
                    Integer.parseInt(cmd.getOptionValue("connectionintervallimit"));
        }
        if (cmd.hasOption("secret")) {
            Server.secret = cmd.getOptionValue("secret");
        } else {    //Randomly generate a string sequence and remove hyphens
            Server.secret = UUID.randomUUID().toString().replaceAll("-", "");
        }
        
        System.out.println(new Timestamp(System.currentTimeMillis()) 
                + " - [INFO] - Starting the EZShare Server\n"); 
        System.out.println(new Timestamp(System.currentTimeMillis()) 
                + " - [INFO] - using secret: " + Server.secret + "\n");
        System.out.println(new Timestamp(System.currentTimeMillis()) 
                + " - [INFO] - using advertised hostname: " + Server.hostname + "\n");
        
        //factory for server sockets
        ServerSocketFactory factory = ServerSocketFactory.getDefault();
        
        //Create server socket that auto-closes, and bind to port
        try (ServerSocket server = factory.createServerSocket(port)){
            System.out.println(new Timestamp(System.currentTimeMillis()) 
                    + " - [INFO] - bound to port " + Server.port);
             
            //Set exchange schema
            TimerTask timerTask = new Exchanger();
    		Timer timer = new Timer(true);
    		timer.scheduleAtFixedRate(timerTask, 1, Server.exchangeIntervalLimit*1000);
            
            //Keep listening for connections...
    		//...and use a thread pool with of max number of threads
            ExecutorService executor = Executors.newFixedThreadPool(MAX_THREADS);
            while (true){
                Socket client = server.accept();
                
                //check connection interval time limit
                String clientIP = client.getInetAddress().toString();

                //if client had connected before...
                if (clientIPList.containsKey(clientIP)) {
                	//...and if enough time has passed...
                    if((System.currentTimeMillis() - clientIPList.get(clientIP)) 
                            > connectionIntervalLimit) {
                        //...update to latest timestamp
                		clientIPList.put(clientIP, System.currentTimeMillis());
                	} else {
                	    //Otherwise it's too soon - close connection!
                		System.out.println(new Timestamp(System.currentTimeMillis())
                		        + " - [WARNING] - " + clientIP + " tried connect too soon.\n");
                		client.close();
                		continue;
                	}
                } else {
                	clientIPList.put(clientIP, System.currentTimeMillis());
                }
                
                connections_cnt++;
                if (debug) {
                    System.out.println(new Timestamp(System.currentTimeMillis()) 
                            + " - [CONN] - Client #" + connections_cnt 
                            + ": " + clientIP + " has connected.");
                }
                //Create, and start, a new thread that processes incoming connections
                executor.submit(new Connection(cmd, client, Server.secret, Server.hostname, Server.port));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    
	private static void PrintValidArgumentList() {
        System.out.println("Valid arguments include: \n"
                + "\t -advertisedhostname <arg>         advertised hostname \n"
                + "\t -connectionintervallimit <arg>    conection interval limit in seconds \n"
                + "\t -exchangeinterval <arg>           exchange interval in seconds \n"
                + "\t -port <arg>                       server port, an integer \n"
                + "\t -secret <arg>                     secret \n"
                + "\t -debug                            print debug information \n");
    }
}