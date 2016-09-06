
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

public class InterfaceJava {

    private ZooKeeper zk = null;

    public InterfaceJava() {
        try {
            zk = new ZooKeeper("admin:2181", 12000, null);
        } catch (IOException e) {
        }
    }

    public static String listServers() throws KeeperException, InterruptedException{
	ClusterMappingEstructure meta = new ClusterMappingEstructure();
	String rootphysic="/Meta/Fisica", servers;
       servers = meta.listServersDirectoriesBuckets(rootphysic);

       return servers;
    }

    public static String listDirectories(String server) throws KeeperException, InterruptedException{
	ClusterMappingEstructure meta = new ClusterMappingEstructure();
	String rootphysic="/Meta/Fisica", diretories;
        diretories = meta.listServersDirectoriesBuckets(rootphysic+server);
	return diretories;
    }

    public static String listBuckets(String path) throws KeeperException, InterruptedException{
	ClusterMappingEstructure meta = new ClusterMappingEstructure();
       String rootphysic="/Meta/Fisica", buckets;
       buckets = meta.listServersDirectoriesBuckets(rootphysic+path);

       return buckets;
    }

    public static void createBucket(String path, int min, int max) throws KeeperException, InterruptedException, UnsupportedEncodingException, Exception{
       ClusterMappingEstructure meta = new ClusterMappingEstructure();
	meta.createBucket(path, min, max);
    }

    public static void createDirectory(String path) throws KeeperException, InterruptedException{
	ClusterMappingEstructure meta = new ClusterMappingEstructure();
	meta.createDirectory(path);
    }

    public static void createServer(String path) throws KeeperException, InterruptedException{
	ClusterMappingEstructure meta = new ClusterMappingEstructure();
	meta.createServer(path);
    }

    public static String getMetadata(String path) throws KeeperException, InterruptedException, UnsupportedEncodingException{
        ClusterMappingEstructure meta = new ClusterMappingEstructure();
        String metadata = meta.getMetadata(path);

        return metadata;
    }

    public static String locateInterval(String interval) throws KeeperException, InterruptedException, Exception{
        IntervalTree t = new IntervalTree();
        String location = t.locateInterval(interval);
        if(location == null){
            System.out.println("Not found!!");
        }
        return location;
    }

    public static String fetchKey(int key) throws KeeperException, InterruptedException, Exception{
        IntervalTree t = new IntervalTree();
        String location = t.fetchKey(key);

        return location;
    }

    public static void deleteBucket(String path) throws InterruptedException, UnsupportedEncodingException, Exception{
         ClusterMappingEstructure a = new ClusterMappingEstructure();
         a.deleteBucket(path);
    }

    public static void rebalancing() throws KeeperException{
        IntervalTree t = new IntervalTree();
        try {
            t.rebalancing();
        } catch (InterruptedException ex) {
            //Logger.getLogger(InterfaceJava.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}

