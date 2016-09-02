package happyangel.learnzookeeper;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Random;

/**
 * Created by happyangel on 16-8-31.
 */
public class Master implements Watcher {
    static ZooKeeper zk;
    static String hostPort;
    static Random random = new Random();
    static String serverId = Integer.toHexString(random.nextInt());
    static boolean isLeader = false;
    AsyncCallback.StringCallback masterCreateCallback = new AsyncCallback.StringCallback() {
        public void processResult(int i, String s, Object o, String s1) {
            switch(KeeperException.Code.get(i)) {
                case CONNECTIONLOSS:
                {
                    checkMaster();
                    return;
                }
                case OK:
                {
                    isLeader = true;
                    break;
                }
                default:
                    isLeader = false;
            }
            System.out.println("I'm the leader");
        }
    };

    AsyncCallback.DataCallback masterCheckCallback = new AsyncCallback.DataCallback() {
        public void processResult(int i, String s, Object o, byte[] bytes, Stat stat) {
            switch (KeeperException.Code.get(i)) {
                case CONNECTIONLOSS: {
                    checkMaster();
                    return;
                }
                case NONODE:
                {
                    runForMaster();
                }
            }
        }
    };

    Master(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    void stopZK() throws InterruptedException {
        zk.close();
    }

    public void process(WatchedEvent e) {
        System.out.println(e);
    }

    void checkMaster() {
       zk.getData("/master", false, masterCheckCallback, null);
    }

    void runForMaster() {
        zk.create("/master", serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, masterCreateCallback, null);
    }


    public static void main(String args[])
            throws Exception {
        Master m = new Master(args[0]);
        m.startZK();

        m.runForMaster();

        if (isLeader) {
            System.out.println("I'm the leader");
            Thread.sleep(60000);
        } else {
            System.out.println("Someone else is the leader");
        }

        m.stopZK();
    }
}

