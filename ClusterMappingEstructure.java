

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;


import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ClusterMappingEstructure {

    private ZooKeeper zk = null;
    private Auxiliary aux = new Auxiliary();
    private IntervalTree val = new IntervalTree();
    private IntervalTree stree = new IntervalTree();
    private final String rootphysic="/Meta/Fisica";
    
    public ClusterMappingEstructure() {
        try {
            zk = new ZooKeeper("admin:2181", 12000, null);
        } catch (IOException e) {
        }
    }
    
    public String listServersDirectoriesBuckets(String node) throws KeeperException, InterruptedException{
        Stat stat = zk.exists(node, true);
        List<String> children;  
        if (stat == null) {
            System.out.println("Path doesn't exist!!");
            return null;
        }
        children = zk.getChildren(node, false); 
        return children.toString().replace("[", "").replace("]", "").replace(",", " -");         
    }
    
    public void createServer(String server) throws KeeperException, InterruptedException{
        System.out.println("Create Server -> : "+server); 
        Stat staServ = zk.exists(rootphysic+"/"+server, true);
        if (staServ == null) {
              byte[] serverData = aux.encodeServerData();
            server=rootphysic+"/"+server;
            try {
            	zk.create(server, serverData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                System.out.println("** Server created! **");
            } catch (InterruptedException ex) {
            	//Logger.getLogger(ClusterMappingEstructure.class.getName()).log(Level.SEVERE, null, ex);
            }
        }else
            System.out.println("Server already exists!");
    }
    
    public void createDirectory(String directory) throws KeeperException, InterruptedException{
	System.out.println("Create Dir -> : "+directory);      	
	String extrem[] = directory.split("/"), server=extrem[0];
        Stat staServ = zk.exists(rootphysic+"/"+server, true);
        Stat staDir = zk.exists(rootphysic+"/"+directory, true);
        if (staServ == null) {
            createServer(server);                         
        }
        if(staDir == null){
            byte[] directoryData = aux.encodeDirectoryData();
            try { 
                directory=rootphysic+"/"+directory;
                zk.create(directory, directoryData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                System.out.println("** Directory created! **");
            } catch (InterruptedException ex) {
                //Logger.getLogger(ClusterMappingEstructure.class.getName()).log(Level.SEVERE, null, ex);
            }
        }else
            System.out.println("Server already exists!");
        aux.close();
    }
    
    public void createBucket(String path, int min, int max) throws KeeperException, InterruptedException, UnsupportedEncodingException, Exception{
	       
	String in = String.valueOf(min)+"-"+String.valueOf(max);
        byte[] data = in.getBytes();
        String interval = Integer.toString(min)+"-"+Integer.toString(max); 
        String location = path;
	String extrem[] = path.split("/"), server=extrem[0], directory=extrem[1];
        
	System.out.println("Bucket path -> "+path); 
    
        Stat staServ = zk.exists(rootphysic+"/"+server, true);
        if (staServ == null) {
            createServer(server);              
        }  
        Stat staDir = zk.exists(rootphysic+"/"+server+"/"+directory, true);
        if (staDir == null) {
            directory= server+"/"+directory; 
            createDirectory(directory);
        }
        path=rootphysic+"/"+path;            
        try {             
            zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            stree.putInterval(location,interval, min, max);
            System.out.println("** Bucket created! **");
        } catch (KeeperException | InterruptedException e) {
        }
        aux.close();
    }
    
    public String getMetadata(String path) throws KeeperException, InterruptedException, UnsupportedEncodingException{
        String data = aux.getMetadata(rootphysic+path);
	if (data == null){
	    System.out.println("No associated data found\n");
            return null;
	} else 
            return data;
    }
    
    public void deleteBucket(String path) throws KeeperException, InterruptedException, UnsupportedEncodingException, Exception{
        Stat stat = zk.exists(rootphysic+path, true);
        if (stat != null) {
            String interval = aux.getMetadata(rootphysic+path);
            String location = val.locateInterval(interval);
            if(location!=null){
                val.cleanInterval(location);
                zk.delete(rootphysic+path, stat.getVersion());
            } 
        }else
            System.out.println("Bucket doesn't exist!!");
    }
}


