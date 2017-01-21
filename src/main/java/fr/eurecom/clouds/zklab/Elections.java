package fr.eurecom.clouds.zklab;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

class Elections {

    private final int processId;
    private final ZooKeeperClient zooKeeperClient;
    public String processNodePath;
    public String watchedNodePath;

    /* Addresses of Zookeeper cluster: DO NOT CHANGE */
    private static final String ZK_QUORUM_ADDRESSES = "192.168.45.15:5181,192.168.45.150:5181,192.168.45.151:5181,192.168.45.152:5181,192.168.45.153:5181";

    Elections(final String groupName, final int processId) throws IOException
    {
        this.processId = processId;

        this.processNodePath = this.watchedNodePath = "";

        this.log("Starting ZooKeeper client...");
        zooKeeperClient = new ZooKeeperClient(ZK_QUORUM_ADDRESSES, groupName, new ZKWatcher());
    }

    void register() {
        // Create an election node if it does not already exists
        String electionPath = zooKeeperClient.createNode("/election", false, false);

        /* ... missing code here: this process must register to participate in the election */
        processNodePath = zooKeeperClient.createNode("/election/n_",processId, true, true);
        log("***** Process ID : " + processNodePath + " *****");
        attemptLeadership();
    }

    void attemptLeadership() {
        /* ... missing code here ...

         the implementation of the leader election goes here.

         Hint: to sort a list of strings you can use Collections.sort(aListOfStrings)

         */
        final List<String> children = zooKeeperClient.getChildren("/election");
        Collections.sort(children);

        int index = children.indexOf(processNodePath.substring(processNodePath.lastIndexOf('/') + 1));
        if(index == 0) {
                log("***** Leader Process: " + processId + " *****");
        } else {
            final String watchedNodeShortPath = children.get(index - 1);
            log("***** Process ID " + processId + " Watched Node Path: " + watchedNodeShortPath + " *****");
            watchedNodePath = "/election/" + watchedNodeShortPath;
            zooKeeperClient.watchNode(watchedNodePath);
        }


    }

    /* ZooKeeper watcher object, the method process() will be called every time a watch is triggered */
    public class ZKWatcher implements Watcher {

        @Override
        public void process(WatchedEvent event)
        {
            log("***** Watch Fired On The Path : " + event.getPath() + " Of Type " + event.getType() + " *****");

            final Event.EventType eventType = event.getType();

            /* ... missing code here ...

               ZooKeeper documentation on event types:
               https://zookeeper.apache.org/doc/r3.4.9/api/org/apache/zookeeper/Watcher.Event.EventType.html
             */

            if(EventType.NodeDeleted.equals(eventType)) {
                if(event.getPath().equalsIgnoreCase("/lab/group07" + watchedNodePath)) {
                    attemptLeadership();
                }
            }

        }
    }

    private void log(String s)
    {
        /* Simple function to prefix the process ID to debugging info */
        System.out.println("***** ID " + this.processId + ": " + s + " *****");
    }
}