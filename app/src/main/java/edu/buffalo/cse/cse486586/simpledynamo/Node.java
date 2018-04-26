package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by sajidkhan on 4/24/18.
 */

public class Node implements Comparable{

    private String nodeId="";
    private String portNo="";

    public Node(){

        nodeId="";
        portNo="";
    }
    public Node(String id, String port)
    {
        nodeId = id;
        portNo = port;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getPortNo() {
        return portNo;
    }

    public void setPortNo(String portNo) {
        this.portNo = portNo;
    }

    @Override
    public int compareTo(Object o) {
        if (!(o instanceof Node))
            throw new ClassCastException();

        Node e = (Node) o;

        return nodeId.compareTo(e.getNodeId());
    }
}
