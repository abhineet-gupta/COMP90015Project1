/*
 * Distributed Systems
 * Group Project 2
 * Sem 1, 2017
 * Group: AALT
 * 
 * Class that handles exchanging servers with others in the list.
 */


package EZShare;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.sql.Timestamp;
import java.util.TimerTask;

import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class Exchanger extends TimerTask{
    private Boolean secure = false;
    private String hostname;
    private int port;
    private int sPort;
    private Boolean debug;
    
 // Normal timeout duration for closing non-persistent connections
    private static final int SOCKET_NORM_TIMEOUT_MS = 2*1000;    //ms
    
    // Timeout duration for connection that should stay open for long
//    private static final int SOCKET_LONG_TIMEOUT_MS = 600*1000;    //ms
    
    public Exchanger(int i, String hostname, int port, int sPort, Boolean debug) {
        this.secure = i==1;
        this.hostname = hostname;
        this.debug = debug;
        this.port = port;
        this.sPort = sPort;
    }

    //Send EXCHANGE command every 10 minutes
    @SuppressWarnings("unchecked")
    @Override
    public void run() {
        ServerList sList = null;
        int hostport = 0;
        if (secure){
            sList = ServerListManager.getSecServerList();
            hostport = this.sPort;
            System.out.println("\n" + new Timestamp(System.currentTimeMillis()) 
                    + " - [INFO] - started secure Exchanger\n");
        } else {
            sList = ServerListManager.getUnsecServerList();
            hostport = this.port;
            System.out.println("\n" + new Timestamp(System.currentTimeMillis()) 
                    + " - [INFO] - started Exchanger\n");
        }
        
        if (sList.getLength() > 0) {
            //select a random server from the list
            JSONObject receiver = sList.select();
            String ip = (String) receiver.get("hostname");
            int port = Integer.parseInt(receiver.get("port").toString());
            
            //connect to it and exchange list of servers
            try {
                
                SSLSocketFactory sslsocketfactory = (SSLSocketFactory) SSLSocketFactory.getDefault();
                SSLSocket sslsocket = null;
                Socket unsecSocket = null;
                DataInputStream input;
                DataOutputStream output;
                
                if (secure){
                    sslsocket = (SSLSocket) sslsocketfactory.createSocket(ip, port);
                  //Get I/O streams for connection
                    input = new DataInputStream(sslsocket.getInputStream());
                    output = new DataOutputStream(sslsocket.getOutputStream());
                    sslsocket.setSoTimeout(SOCKET_NORM_TIMEOUT_MS);
                } else{
                    unsecSocket = new Socket(ip, port);
                    input = new DataInputStream(unsecSocket.getInputStream());
                    output = new DataOutputStream(unsecSocket.getOutputStream());
                    unsecSocket.setSoTimeout(SOCKET_NORM_TIMEOUT_MS);
                }
                
                JSONObject command = new JSONObject();
                command.put("command", "EXCHANGE");
                
                JSONArray serverArr = sList.getCopyServerList();
                JSONObject host = new JSONObject();
                host.put("hostname", hostname);
                host.put("port", hostport);
                serverArr.add(host);

                command.put("serverList", serverArr);
                output.writeUTF(command.toJSONString());
                if (debug) {
                    System.out.println(new Timestamp(System.currentTimeMillis())
                            + " - [DEBUG] - SENT: " + command.toJSONString());
                }
                output.flush();
                String recv_response;
                while(true) {
                    try {
                        if ((recv_response = input.readUTF()) != null) {
                            
                            if (debug) {
                                System.out.println(new Timestamp(System.currentTimeMillis())
                                        + " - [DEBUG] - RECEIVED: " + recv_response);
                            }
                        }
                    } catch (SocketException e) {
                        if (debug) {
                            System.out.println(new Timestamp(System.currentTimeMillis())
                                    +" - [FINE] - (Exchanger) Connection closed by other side.");
                        }
                        break;
                    } catch (SocketTimeoutException e){ //socket timed out
                        if (debug) {
                            System.out.println(new Timestamp(System.currentTimeMillis())
                                    +" - [FINE] - (Exchanger) Connection closed.");
                        }
                        break;
                    }
                }
                
                if (secure){sslsocket.close();}
                else {unsecSocket.close();}
                
            } catch (ConnectException e) {
                System.out.println(new Timestamp(System.currentTimeMillis())
                        + " - [ERROR] - Connection timed out.");
                sList.remove(receiver);
            } 
            catch (IOException e) {
                System.out.println(new Timestamp(System.currentTimeMillis())
                        + " - [ERROR] - IO Exception occurred.");
                sList.remove(receiver);
            }
        }       
    }
}
