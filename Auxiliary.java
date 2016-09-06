


import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Scanner;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class Auxiliary {

    private ZooKeeper zk = null;

    public Auxiliary() {
        try {
            zk = new ZooKeeper("admin:2181", 12000, null);
        } catch (IOException e) {
        }
    }

    public void close() {
        try {
            zk.close();
        } catch (InterruptedException e) {
        }
     }

    public byte[] encodeServerData(){
        Scanner scan = new Scanner(System.in);
        String padraoIp = "\\d{0,9}.\\d{0,9}.\\d{0,9}.\\d{0,9}", ip=null;
        do{
        System.out.println("Insert server IP address (127.0.0.1): ");
        ip = scan.nextLine();
        }while(!ip.matches(padraoIp));
        return ip.getBytes();
    }

    public byte[] encodeDirectoryData(){
        Scanner scan = new Scanner(System.in);
        System.out.println("Insert diretory discription: !!");
        String data = scan.nextLine();
        return data.getBytes();
    }

    public static byte[] encodeIntervalMetadata(int subMax, int min, int max, String path) throws Exception {
        ByteBuffer buff = ByteBuffer.allocate(Integer.SIZE/8 + Integer.SIZE/8 + Integer.SIZE/8 + Character.SIZE/8 * path.length());
        buff
            .putInt(subMax)
            .putInt(min)
            .putInt(max)
            .asCharBuffer()
            .append(path);
        return buff.array();
    }
    public static int dumpSubTreeMaxpoint(byte[] bytes) throws Exception {
        ByteBuffer buff = ByteBuffer.wrap(bytes);
        int subTreeMax    = buff.getInt();
        int min    = buff.getInt();
        int max    = buff.getInt();
        String p = buff.asCharBuffer().toString();
        return subTreeMax;
    }

    public static int dumpIntervalZnodeMin(byte[] bytes) throws Exception {
        ByteBuffer buff = ByteBuffer.wrap(bytes);
        int subTreeMax    = buff.getInt();
        int min    = buff.getInt();
        int max    = buff.getInt();
        String p = buff.asCharBuffer().toString();
        return min;
    }

    public static int dumpIntervalZnodeMax(byte[] bytes) throws Exception {
        ByteBuffer buff = ByteBuffer.wrap(bytes);
        int subTreeMax    = buff.getInt();
        int min    = buff.getInt();
        int max    = buff.getInt();
        String p = buff.asCharBuffer().toString();
        return max;
    }

     public static String dumpIntervalZnodePath(byte[] bytes) throws Exception {
        ByteBuffer buff = ByteBuffer.wrap(bytes);
        int subTreeMax    = buff.getInt();
        int min    = buff.getInt();
        int max    = buff.getInt();
        String path = buff.asCharBuffer().toString();
        return path;
    }

    public String getMetadata(String znode) throws UnsupportedEncodingException, KeeperException, InterruptedException{
        Stat stat;
        byte [] buff;
        String data;
        stat = zk.exists(znode, false);
        if (stat == null) {
            System.out.println("Znode doesn't exist!");
            return null;
        }else{
            buff=zk.getData(znode, false, stat);
            data = new String(buff, "UTF-8");
            System.out.println("Data -- "+data);
            return data;
        }
    }
}

