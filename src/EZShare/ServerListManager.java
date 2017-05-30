package EZShare;

public class ServerListManager {
    private static ServerList unsecServerList = new ServerList();
    private static ServerList secServerList = new ServerList();
    
    public static ServerList getUnsecServerList() {
        return unsecServerList;
    }
    
    public static ServerList getSecServerList() {
        return secServerList;
    }
}
