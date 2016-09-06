

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class IntervalTree {
    private ZooKeeper zk = null;
    private Auxiliary aux = new Auxiliary();
    private final String treeroot = "/Meta/Busca";
    List removeList = new ArrayList();
    List dataList = new ArrayList();

    public IntervalTree() {
        try {
            zk = new ZooKeeper("admin:2181", 12000, null);
        } catch (IOException e) {
        }
    }

    public void putInterval(String path, String interval, int min, int max) throws KeeperException, InterruptedException, Exception {
        String znode = treeroot;
        String intervalExtrems[]= interval.split("-");
        List<String> children = zk.getChildren(znode, false);
        int n_child = children.size();
        //Search tree without root interval node
        if (n_child == 0) {
            byte[] data = aux.encodeIntervalMetadata(max, min, max, path);
            znode = znode + "/" + interval;
            try {
                zk.create(znode, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                System.out.println("Root Created!");
            } catch (InterruptedException ex) {
                //Logger.getLogger(IntervalTree.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        //Search tree has a root interval node
        else if (n_child == 1) {
            insertInterval(znode + "/" + children.get(0), interval, min, max, path);
        }
    }

    private void insertInterval(String znode, String interval, int min, int max, String path) throws InterruptedException, KeeperException, Exception {
        byte[] metadata, metadataInserted, data, znodeMetadata;
        String oldPath = null;
        znodeMetadata = zk.getData(znode, null, null);

        int minZnode = aux.dumpIntervalZnodeMin(znodeMetadata);
        int maxZnode = aux.dumpIntervalZnodeMax(znodeMetadata);
        int intervalMinPoint = min, intervalMaxPoint = max;

        /* Update the subtree maximum point */
        int oldMaximumPoint = 0;

        data = zk.getData(znode, null, null);
        oldPath = aux.dumpIntervalZnodePath(data);
        oldMaximumPoint = aux.dumpSubTreeMaxpoint(data);


        if (intervalMaxPoint > oldMaximumPoint) {
            int buffmin = aux.dumpIntervalZnodeMin(data),
            buffmax = aux.dumpIntervalZnodeMax(data);
            metadata = aux.encodeIntervalMetadata(intervalMaxPoint, buffmin, buffmax, oldPath);
            try {
                zk.setData(znode, metadata, -1);
            } catch (KeeperException ex) {
                //Logger.getLogger(IntervalTree.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        List<String> children = zk.getChildren(znode, false);

        int n_child = children.size();

        if (n_child == 0) {
            metadataInserted = aux.encodeIntervalMetadata(intervalMaxPoint, min, max, path);
            znode = znode + "/" + interval;
            try {
                zk.create(znode, metadataInserted, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                System.out.println("Interval Created!");
            } catch (InterruptedException ex) {
                //Logger.getLogger(IntervalTree.class.getName()).log(Level.SEVERE, null, ex);
            }
        } else if (n_child == 1) {
            String child = children.get(0);
            String znodeChild = znode + "/" + child;
            byte[] metadataChild = zk.getData(znodeChild, null, null);
            int minChild = aux.dumpIntervalZnodeMin(metadataChild),
                maxChild = aux.dumpIntervalZnodeMax(metadataChild);

            if (intervalMinPoint > maxZnode && minZnode > maxChild) {
                metadataInserted = aux.encodeIntervalMetadata(intervalMaxPoint, min, max,path);
                znode = znode + "/" + interval;
                try {
                    zk.create(znode, metadataInserted, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (InterruptedException ex) {
                    //Logger.getLogger(IntervalTree.class.getName()).log(Level.SEVERE, null, ex);
                }
            } else if (intervalMaxPoint < minZnode && maxZnode < minChild) {
                metadataInserted = aux.encodeIntervalMetadata(intervalMaxPoint, min, max, path);
                znode = znode + "/" + interval;
                try {
                    zk.create(znode, metadataInserted, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (InterruptedException ex) {
                    //Logger.getLogger(IntervalTree.class.getName()).log(Level.SEVERE, null, ex);
                }
            } else if (intervalMaxPoint < minChild && maxChild < minZnode) {
                znode = znode + "/" + child;
                insertInterval(znode, interval, min, max, path);
            } else if (intervalMinPoint > maxChild && intervalMaxPoint < minZnode) {
                znode = znode + "/" + child;
                insertInterval(znode, interval, min, max, path);
            } else if (intervalMinPoint > maxZnode && intervalMaxPoint < minChild) {
                znode = znode + "/" + child;
                insertInterval(znode, interval, min, max, path);
            } else if (intervalMinPoint > maxChild && minChild > maxZnode) {
                znode = znode + "/" + child;
                insertInterval(znode, interval,min, max, path);
            }else
                System.out.println("Not implemented!");

        } else if (n_child == 2) {
            String child1 = children.get(1), child2 = children.get(0);
            String znodeChild1 = znode + "/" + child1;
            String znodeChild2 = znode + "/" + child2;
            byte[] metadataChild1 = zk.getData(znodeChild1, null, null);
            byte[] metadataChild2 = zk.getData(znodeChild2, null, null);

            int minChild1 = aux.dumpIntervalZnodeMin(metadataChild1),
                maxChild1 = aux.dumpIntervalZnodeMax(metadataChild1);
            int minChild2 = aux.dumpIntervalZnodeMin(metadataChild2),
                maxChild2 = aux.dumpIntervalZnodeMax(metadataChild2);

            /*Left case left*/
            if(intervalMaxPoint < minChild1 && maxChild1 < minZnode && maxZnode < minChild2 ){
                insertInterval(znodeChild1, interval,min, max, path);
            } else  if(intervalMaxPoint < minChild2 && maxChild2 < minZnode && maxZnode < minChild1 ){
                insertInterval(znodeChild2, interval,min, max, path);
            }
            /*Left case right*/
            else if(intervalMinPoint > maxChild1 && intervalMaxPoint < minZnode && maxZnode < minChild2) {
                insertInterval(znodeChild1, interval, min, max, path);
            } else if(intervalMinPoint > maxChild2 && intervalMaxPoint < minZnode && maxZnode < minChild1) {
                insertInterval(znodeChild2, interval,min, max, path);
            }
            /*Right case left*/
            else if(intervalMinPoint > maxZnode && minZnode > maxChild1 && intervalMaxPoint < minChild2) {
                insertInterval(znodeChild2, interval,min, max, path);
            } else if(intervalMinPoint > maxZnode && minZnode > maxChild2 && intervalMaxPoint < minChild1) {
              insertInterval(znodeChild1, interval,min, max, path);

             /*Right case right*/
            } else if(intervalMinPoint > maxChild2 && minChild2 > maxZnode && minZnode > maxChild1) {
              insertInterval(znodeChild2, interval,min, max, path);
            } else if(intervalMinPoint > maxChild1 && minChild1 > maxZnode && minZnode > maxChild2) {
              insertInterval(znodeChild1, interval,min, max, path);
            } else
              System.out.println("Didn't work!");
        }
    }

    public String fetchKey(int key) throws KeeperException, InterruptedException, Exception {
        String znode = treeroot, locate = null;
        List<String> children = zk.getChildren(znode, false);
        int childrenNumber = children.size();

        if (childrenNumber == 0) {
            System.out.println("Key not found!!!");
            System.exit(0);
        }
        znode = znode + "/" + children.get(0);
        locate = searchKey(key, znode);
        return locate;
    }

    private String searchKey(int key, String znode) throws KeeperException, InterruptedException, Exception {
       String locate = null;
       byte[] znodeMetadata = zk.getData(znode, null, null);
       int max = aux.dumpSubTreeMaxpoint(znodeMetadata),
       minInterval = aux.dumpIntervalZnodeMin(znodeMetadata),
       maxInterval = aux.dumpIntervalZnodeMax(znodeMetadata);

       String location = aux.dumpIntervalZnodePath(znodeMetadata);

        if (key > max) {
            System.out.println("Key doesn't exist!!");
            System.exit(0);
        } else if (key >= minInterval && key <= maxInterval) {
            locate = location;
        } else {
            List<String> children = zk.getChildren(znode, false);
            int chlidrenNumber = children.size();

            if (chlidrenNumber == 0) {
                System.out.println("Key not found!!");
                System.exit(0);
            } else if (chlidrenNumber == 1) {
                znode = znode + "/" + children.get(0);
                locate = searchKey(key, znode);
            } else if (chlidrenNumber == 2) {

                String child1 = children.get(0), child2 = children.get(1);

                String znodeChild1 = znode +"/"+ child1,
                       znodeChild2 = znode +"/"+ child2;

                int min1 = aux.dumpIntervalZnodeMin(zk.getData(znodeChild1, null, null)),
                    min2 = aux.dumpIntervalZnodeMin(zk.getData(znodeChild2, null, null));

                if (key < minInterval && min1 < min2) {
                    locate = searchKey(key, znodeChild1);
                }
                if (key < minInterval && min2 < min1) {
                    locate = searchKey(key, znodeChild2);
                }
                if (key > minInterval && min1 > min2) {
                    locate = searchKey(key, znodeChild1);
                }
                if (key > minInterval && min2 > min1) {
                    locate = searchKey(key, znodeChild2);
                }
            }
        }
        return locate;
    }

    public String locateInterval(String interval) throws UnsupportedEncodingException, KeeperException, InterruptedException, Exception{
        List<String> root = zk.getChildren(treeroot, false);
        if(root!=null){
            String node = treeroot+"/"+root.toString().replace("[", "").replace("]","");
           return intervalLocation(node,interval);
        } else
            return null;
    }

   private String intervalLocation(String node, String interval) throws KeeperException, InterruptedException, UnsupportedEncodingException, Exception {
        List<String> children = zk.getChildren(node, false);
        int num = children.size();
        String z = node.split("/")[node.split("/").length-1];

        if(z.equals(interval)){
            return node;
        }
        else if(num == 1){
            String uniqueZnode = node+"/"+children.toString().replace("[", "").replace("]","");
            return intervalLocation(uniqueZnode,interval);
        }
        else if(num == 2){
            String first = node+"/"+children.get(0).replace("[", "").replace("]","");
            String second = node+"/"+children.get(1).replace("[", "").replace("]","");
            String extInterval[] = interval.split("-");

            byte[] buff1 =zk.getData(first, null, null);
            byte[] buff2 =zk.getData(second, null, null);

            int maxpF = aux.dumpSubTreeMaxpoint(buff1);
            int maxpS = aux.dumpSubTreeMaxpoint(buff2);

            int c = Integer.parseInt(extInterval[1]);

            if (c <= maxpF && maxpF < maxpS) {
                node = first;
                return intervalLocation(node,interval);
            } else if (c <= maxpS && maxpS < maxpF) {
                node = second;
                return intervalLocation(node,interval);
            } else if (c <= maxpF && maxpF > maxpS) {
                node = first;
                return intervalLocation(node,interval);
            } else if (c <= maxpS && maxpS > maxpF) {
                node = second;
                return intervalLocation(node,interval);
            }
            else {
                return "Not implemented yet!!!";
            }
        }
        return null;
    }

    public void cleanInterval(String location) throws Exception {
       int intervalMaxPoint = aux.dumpSubTreeMaxpoint(zk.getData(location, null, null));
       int min = aux.dumpIntervalZnodeMin(zk.getData(location, null, null));
       int max = aux.dumpIntervalZnodeMax(zk.getData(location, null, null));
       byte[] data = aux.encodeIntervalMetadata(intervalMaxPoint, min, max, "");
       Stat stat = new Stat();
       zk.setData(location, data, stat.getVersion());
    }

    public void rebalancing () throws KeeperException, InterruptedException{
          List<String> root = zk.getChildren(treeroot, false);
          String node = treeroot+"/"+root.toString().replace("[", "").replace("]","");
    }

    public void constroyRemoveList(String node) throws KeeperException, InterruptedException, Exception{
        List<String> children = zk.getChildren(node, false);
        List lista = new ArrayList();

        String metadata = aux.dumpIntervalZnodePath(zk.getData(node, null, null));

        if(metadata.equals("")){
           lista.add(node);
        }
        int num = children.size();
        Stat stat = new Stat();
        if(num==1){
            String unique = node+"/"+children.get(0).replace("[", "").replace("]","");
            constroyRemoveList(unique);
        }
        else if(num==2){
            String child1 = node+"/"+children.get(0).replace("[", "").replace("]","");
            String child2 = node+"/"+children.get(1).replace("[", "").replace("]","");
            constroyRemoveList(child1);
            constroyRemoveList(child2);
        }
        else if (num == 0){
            zk.delete(node, stat.getVersion());
        }
        for (Object lista1 : lista) {
            removeList.add(lista1);
        }
    }

    public void removeReinsert(String a) throws UnsupportedEncodingException, InterruptedException, KeeperException, Exception{
        Stat stat = new Stat();
        for (Object lista1 : removeList) {
            if(!lista1.equals(a)){
                String buffData = aux.getMetadata(lista1.toString());
                String buff[] = buffData.split("-"), data=buff[1];
                dataList.add(data);
            }
        }
        for (int i=0; i<removeList.size();i++) {
            try {
                stat = zk.exists(removeList.get(i).toString(), true);
                if (stat != null) {
                    zk.delete(removeList.get(i).toString(), stat.getVersion());
                } else
                    System.out.println("Node doesn't exist");
            } catch (KeeperException e) {
                    System.out.println("Error: "+e);
            }
        }

        for (int i=0;i<dataList.size();i++) {
            try {
                stat = zk.exists(dataList.get(i).toString(), true);

                if (stat == null) {
                    String buff[] = removeList.get(i).toString().split("/");
                    byte[] data = zk.getData(removeList.get(i).toString(), null, null);
                    int min = aux.dumpIntervalZnodeMin(data);
                    int max = aux.dumpIntervalZnodeMax(data);
                    int n = buff.length;
                    String interval=buff[n-1];
                    putInterval(dataList.get(i).toString(),interval, min, max);
                } else
                     System.out.println("Node doesn't exist");
            } catch (KeeperException e) {
                System.out.println("Error: "+e);
            }
        }
    }
}

