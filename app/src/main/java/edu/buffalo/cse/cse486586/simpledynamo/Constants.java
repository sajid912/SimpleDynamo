package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by sajidkhan on 4/24/18.
 */

public class Constants {


    public static final String EMULATOR0_PORT = "11108";
    public static final String EMULATOR1_PORT = "11112";
    public static final String EMULATOR2_PORT = "11116";
    public static final String EMULATOR3_PORT = "11120";
    public static final String EMULATOR4_PORT = "11124";


    public static final String EMULATOR0_ID = "5554";
    public static final String EMULATOR1_ID = "5556";
    public static final String EMULATOR2_ID = "5558";
    public static final String EMULATOR3_ID = "5560";
    public static final String EMULATOR4_ID = "5562";

    public static final String DUMMY_REPLY = "dummyReply";
    public static final String INSERT_FORWARD_REQUEST = "insertForwardRequest";
    public static final String INSERT_REPLICA_REQUEST = "insertReplicaRequest";
    public static final String INSERT_REPLICA_REPLY = "insertReplicaReply";

    public static final int SERVER_PORT = 10000;

    public static final String DATA_ORIGIN_PORT = "dataOriginPort";

    public static final String QUERY_ORIGIN_PORT = "queryOriginPort";
    public static final String DATA_QUERY_REQUEST = "dataQueryRequest";
    public static final String QUERY_REPLY_PORT = "queryReplyPort";

    public static final String KEY = "key";
    public static final String VALUE = "value";
    public static final String STATUS = "status";
    public static final String REPLICA_INDEX = "replicaIndex";

    public static final String ALL_DATA_QUERY_REQUEST = "allDataQueryRequest";
    public static final String ALL_DATA_QUERY_REPLY = "allDataQueryReply";

    public static final String TEXT_SEPARATOR = "===";
    public static final String INSERT_DELETE_SEPARATOR = "+-+";
    public static final String VERSION_SEPARATOR = "SAJIDKHA";
    public static final String KEY_VALUE_SEPARATOR = ";";
    public static final String ALL_DATA_QUERY_CONTENT = "allDataQueryContent";

    public static final String ALL_DATA_DELETE_REQUEST = "allDataDeleteRequest";
    public static final String SINGLE_DATA_DELETE_REQUEST = "singleFileDeleteRequest";

    public static final String DELETE_REPLICA_REQUEST = "deleteReplicaRequest";

    public static final String COORDINATOR_PORT = "coordinatorPort";
    public static final String SAVED_LOCALLY = "savedLocally";

    public static final String REPLICATION_DONE = "replicationDone";

    public static final String FAILURE_CASES = "failureCases";

    public static final String DIRECT_INSERT_REPLICA_REQUEST = "directInsertReplicaRequest";
    public static final String DIRECT_INSERT_REPLICA_REPLY = "directInsertReplicaReply";

    public static final String ASK_MISSING_KEYS = "askMissingKeys";
    public static final String ASKING_PORT = "askingPort";
    public static final String KEY_TYPE = "keyType";

    public static final String NO_KEYS = "noKeys";
    public static final String KEY_CONTENT = "keyContent";

    public static final String FIRST_REPLICA_PORT = "firstReplicaPort";
    public static final String SECOND_REPLICA_PORT = "secondReplicaPort";

    public static final String TAIL_PORT = "tailPort";
    public static final String DIRECT_DELETE_REPLICA_REQUEST = "directDeleteReplicaRequest";


    // so far
    public static final String PREDECESSOR_PORT = "predecessorPort";
    public static final String SUCCESSOR_PORT = "successorPort";

    public static final String MY_PORT = "my_port";
    public static final String MY_NODE_ID = "nodeId";

    public static final String JOIN_REQUEST = "joinRequest";
    public static final String UPDATE_NEIGHBORS = "updateNeighbors";

    public static final String UPDATE_NEIGHBORS_LOCAL = "updateNeighborsLocally";
    public static final String UPDATE_NEIGHBORS_REMOTE = "updateNeighborsRemote";
    public static final String DATA_QUERY_REPLY = "dataQueryReply";








    public static final String[] ALL_PORTS = {EMULATOR0_PORT, EMULATOR1_PORT, EMULATOR2_PORT, EMULATOR3_PORT, EMULATOR4_PORT };


    public static final String COUNT = "count";

    public static final String NODE_LIST_CONTENT = "nodeListContent";
    public static final String NODE_LIST_QUERY_REQUEST = "nodeListQueryRequest";
    public static final String NODE_LIST_QUERY_REPLY = "nodeListQueryReply";


}
