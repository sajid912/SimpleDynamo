package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.widget.Toast;

import org.json.JSONException;
import org.json.JSONObject;

import static android.content.ContentValues.TAG;

public class SimpleDynamoProvider extends ContentProvider {


    private ArrayList<Node> nodeList = new ArrayList<Node>();
    private Node myNode;
    private boolean globalFlag = false;
    private String globalValue = "";
    private Object valueLock = new Object();
    private boolean insertFlag = false;
    private Object insertLock = new Object();
    private ArrayList<String> allDataQueryReplyList = new ArrayList<String>();
    private int allDataQueryReplyCount = 0;
    private int allDataQueryActivePorts = 4;
    private HashMap<String, Integer> insertReplicaHashMap = new HashMap<String, Integer>();
    private HashMap<String, Integer> liveReplicaHashMap = new HashMap<String, Integer>();
    private HashMap<String, Integer> directInsertHashMap = new HashMap<String, Integer>();

    private HashMap<String, String> coordinatorHashMap = new HashMap<String, String>();
    private HashMap<String, String> successor1HashMap = new HashMap<String, String>();
    private HashMap<String, String> successor2HashMap = new HashMap<String, String>();

    private int missingKeysReplyCount = 0;
    private int missingKeysActivePorts = 3; // Standard value

    @Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub

        Log.v("delete", selection);

        try {

            if(selection.equals("*")) // delete all key value pairs in entire DHT
            {
                handleAllDataDelete();
            }
            else if(selection.equals("@")) // delete key value pairs in the local node
            {
                deleteAllLocalFiles();
            }
            else // other cases
            {
                handleOtherDeleteCases(selection);
            }

        } catch (Exception e) {
            Log.e(TAG, "File write failed");
        }

		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub

        try {

            synchronized (insertLock) {
                while ( insertFlag != true ) {

                    Log.v("insert", values.toString());
                    String key = values.getAsString("key");
                    String value = values.getAsString("value");

                    insertValues(key, value);
                    insertLock.wait();
                }
                // value is now true
                insertFlag = false;
            }
        } catch ( InterruptedException x ) {
            Log.d(TAG,"interrupted while waiting");
        }

		return uri;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
        onNodeStart();
		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub

        Log.v("query", selection);

        try {

            if(selection.equals("*")) // return all key value pairs
            {

                MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
                return handleAllDataQuery(matrixCursor);

            }
            else if(selection.equals("@")) // return key value pairs in the local node
            {
                int index = 0;
                MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});

                for(File file: getContext().getFilesDir().listFiles())
                {
                    String fileName = file.getName();
                    String fileValue = getFileContentFromName(fileName);
                    matrixCursor.addRow(new Object[]{fileName, fileValue});
                    Log.d(TAG,"Each local file name:"+fileName);
                    index++;
                }

                Log.d(TAG,"No of local files:"+index);
                return matrixCursor;
            }
            else // other cases
            {
                MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
                return handleOtherQueryCases(matrixCursor, selection);

            }

        } catch (Exception e) {
            Log.e(TAG, "File write failed");
            e.printStackTrace();
        }


        return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private String getEmulatorId()
    {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        return String.valueOf((Integer.parseInt(portStr)));
    }

    private void initializeNodeList()
    {
        try {
            nodeList.add(new Node(genHash(Constants.EMULATOR0_ID), Constants.EMULATOR0_PORT));
            nodeList.add(new Node(genHash(Constants.EMULATOR1_ID), Constants.EMULATOR1_PORT));
            nodeList.add(new Node(genHash(Constants.EMULATOR2_ID), Constants.EMULATOR2_PORT));
            nodeList.add(new Node(genHash(Constants.EMULATOR3_ID), Constants.EMULATOR3_PORT));
            nodeList.add(new Node(genHash(Constants.EMULATOR4_ID), Constants.EMULATOR4_PORT));

            Collections.sort(nodeList);

            String myEmulatorId = getEmulatorId();
            myNode = new Node(genHash(myEmulatorId), getPortNumber(myEmulatorId));
        }
        catch (NoSuchAlgorithmException e){
            e.printStackTrace();
        }


    }

    private void onNodeStart()
    {
        try {

            ServerSocket serverSocket = new ServerSocket(); // <-- create an unbound socket first
            serverSocket.setReuseAddress(true);
            serverSocket.bind(new InetSocketAddress(Constants.SERVER_PORT));
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            e.printStackTrace();
            return;
        }

        coordinatorHashMap.clear();
        successor1HashMap.clear();
        successor2HashMap.clear();

        deleteAllLocalFiles();

        initializeNodeList();
        askForMissingKeys();

//        try {
//            synchronized (insertLock) {
//
//                while (insertFlag!= true)
//                {
//                    // Also check if you missed any keys, ask predecessors and successors
//                    askForMissingKeys();
//                    insertLock.wait();
//                }
//
//                insertFlag = false; // reset the flag
//            }
//        } catch (InterruptedException x) {
//            Log.d(TAG,"interrupted while waiting");
//
//        }

    }

    private void insertValues(String key, String value)
    {
        boolean forward = false;
        String coordinatorPortNo="";

        try {

            Log.d(TAG,"Key:"+key);
            String hashedKey = genHash(key);
            coordinatorPortNo = getCoordinatorPortNo(hashedKey);
            Log.d(TAG,"Coordinator port no:"+coordinatorPortNo);
            if(!coordinatorPortNo.equals(getPortNumber(getEmulatorId()))) // If you are not coordinator, forward to coordinator
                forward = true;

        }
        catch (NoSuchAlgorithmException e){
            e.printStackTrace();
        }

        if(forward)
        {
            if(!coordinatorPortNo.isEmpty())
            {
                Log.d(TAG,"Forwarding the insert");
                new clientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, coordinatorPortNo,
                        constructDataObject(key, value, 1), Constants.INSERT_FORWARD_REQUEST); // 1 stands for insert forward request
            }
            else
                Log.e(TAG,"Coordinator port no cant be empty");

        }
        else
        {
            Log.d(TAG,"Trying to save locally");
            saveLocally(key, value);
            coordinatorHashMap.put(key, value);
            insertReplicaHashMap.put(key+Constants.TEXT_SEPARATOR+myNode.getPortNo(), 0);
            liveReplicaHashMap.put(key+Constants.TEXT_SEPARATOR+myNode.getPortNo(), 2); // default 2 replicas
            // Also tell next two successor to save the key, value pair
            saveReplicaInSuccessors(key, value, myNode.getPortNo(), myNode.getPortNo());
        }
    }

    private boolean saveLocally(String key, String value)
    {
        try {
            value = value + "\n";
            FileOutputStream outputStream;
            outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
            outputStream.write(value.getBytes());
            outputStream.close();
            return true;

        } catch (Exception e) {
            Log.e(TAG, "Save local failed");
            return false;
        }
    }

    private String getCoordinatorPortNo(String hashedKey)
    {
        String coordinatorPortNo="";

        if(hashedKey.compareTo(nodeList.get(0).getNodeId())<0
                || hashedKey.compareTo(nodeList.get(nodeList.size()-1).getNodeId())>0)
        {
            // Hashed key is smaller than smaller node or larger than larger node - special cases
            coordinatorPortNo = nodeList.get(0).getPortNo();
        }
        else {
            // other cases
            for(int i=0;i<nodeList.size();i++)
            {
               if(hashedKey.compareTo(nodeList.get(i).getNodeId())>0 && hashedKey.compareTo(nodeList.get(i+1).getNodeId())<0)
               {
                   coordinatorPortNo = nodeList.get(i+1).getPortNo();
                   break;
               }

            }

        }

      return coordinatorPortNo;
    }

    public String getPortNumber(String emulatorId)
    {
        int a = Integer.parseInt(emulatorId);
        int port_no = 2*a;
        return String.valueOf(port_no);
    }

    private String constructDataObject(String key, String value, int status)
    {
        JSONObject jsonObject = new JSONObject();

        try {
            jsonObject.put(Constants.STATUS, status);
            jsonObject.put(Constants.DATA_ORIGIN_PORT, myNode.getPortNo());
            jsonObject.put(Constants.KEY, key);
            jsonObject.put(Constants.VALUE, value);

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return jsonObject.toString();
    }

    private String constructInsertReplicaObject(String key, String value, String dataOriginPort, int index)
    {
        JSONObject jsonObject = new JSONObject();

        try {
            jsonObject.put(Constants.STATUS, 2);
            jsonObject.put(Constants.DATA_ORIGIN_PORT, dataOriginPort);
            jsonObject.put(Constants.KEY, key);
            jsonObject.put(Constants.VALUE, value);
            jsonObject.put(Constants.REPLICA_INDEX, index);

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return jsonObject.toString();
    }

    private String constructQueryObject(String key, String coordinatorPortNo, String firstSucc, String secondSucc, String tailPort)
    {
        JSONObject jsonObject = new JSONObject();

        try {
            jsonObject.put(Constants.STATUS, 3); // 3 status for data query request
            jsonObject.put(Constants.QUERY_ORIGIN_PORT, myNode.getPortNo());
            jsonObject.put(Constants.COORDINATOR_PORT, coordinatorPortNo);
            jsonObject.put(Constants.FIRST_REPLICA_PORT, firstSucc);
            jsonObject.put(Constants.SECOND_REPLICA_PORT, secondSucc);
            jsonObject.put(Constants.TAIL_PORT, tailPort);
            jsonObject.put(Constants.KEY, key);

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return jsonObject.toString();
    }

    private String constructQueryReplyObject(String key, String value)
    {
        try {

            JSONObject replyJsonObject = new JSONObject();
            replyJsonObject.put(Constants.STATUS, 4); // 4 for data query reply
            replyJsonObject.put(Constants.KEY, key);
            replyJsonObject.put(Constants.VALUE, value);
            replyJsonObject.put(Constants.QUERY_REPLY_PORT, myNode.getPortNo());

            return replyJsonObject.toString();

        } catch (JSONException e) {
            e.printStackTrace();
        }

        return "";
    }

    private ArrayList<String> getSuccessorPorts(String coordinatorPortNo)
    {
        ArrayList<String> ports= new ArrayList<String>();

        for(int i=0;i<nodeList.size();i++)
        {
            if(nodeList.get(i).getPortNo().equals(coordinatorPortNo))
            {
                if(i==nodeList.size()-2)
                {
                    ports.add(nodeList.get(nodeList.size()-1).getPortNo());
                    ports.add(nodeList.get(0).getPortNo());
                }
                else if(i==nodeList.size()-1)
                {
                    ports.add(nodeList.get(0).getPortNo());
                    ports.add(nodeList.get(1).getPortNo());
                }
                else
                {
                    ports.add(nodeList.get(i+1).getPortNo());
                    ports.add(nodeList.get(i+2).getPortNo());
                }

                break;
            }
        }

        for(String s: ports)
            Log.d(TAG,"Successor ports:"+s);

        return ports;
    }

    private String getFileContentFromName(String fileName)
    {

        try {
            FileInputStream inputStream = getContext().openFileInput(fileName);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String value = bufferedReader.readLine();
            Log.d(TAG, "Value is:" + value);
            bufferedReader.close();
            return value;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }

    private void saveReplicaInSuccessors(String key, String value, String dataOriginPort, String coordinatorPortNo)
    {
        int index=1;
        for(String successorPort: getSuccessorPorts(coordinatorPortNo))
        {
            new clientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, successorPort,
                    constructInsertReplicaObject(key, value, dataOriginPort, index), Constants.INSERT_REPLICA_REQUEST);
            // 2 stands for insert replica request
            index++;
        }
    }

    private MatrixCursor handleOtherQueryCases(MatrixCursor matrixCursor, String selection)
    {
        // query is asking for the key just inserted
        // If insert operation is in progress - block on query
        // Unblock when insert finishes and proceed with query

//        try {
//            synchronized (insertLock) {
//                while (insertFlag!= true)
//                {
//                    matrixCursor = handleOtherQueryCases1(matrixCursor, selection);
//                    insertLock.wait();
//                }
//            }
//
//        } catch ( InterruptedException x ) {
//            Log.d(TAG,"interrupted while waiting");
//        }

        return handleOtherQueryCases1(matrixCursor, selection);
    }

    private MatrixCursor handleOtherQueryCases1(MatrixCursor matrixCursor, String selection)
    {
        try {
            synchronized (valueLock) {
                while ( globalFlag != true ) {

                    String hashedKey = genHash(selection);
                    String coordinatorPortNo = getCoordinatorPortNo(hashedKey);

                    ArrayList<String> successorPorts = getSuccessorPorts(coordinatorPortNo);

                    // chain replication - reads at tail, inserts at the head
                    // Now ask the successors for query, starting with second successor first
                    // In case second succ fails, ask the first succ

                    String secondSuccessorPortNo = successorPorts.get(1); // Hardcoded bocz there are only two successors

                    new clientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, secondSuccessorPortNo,
                            constructQueryObject(selection, coordinatorPortNo, successorPorts.get(0),
                                    successorPorts.get(1), successorPorts.get(1)), Constants.DATA_QUERY_REQUEST);
                    valueLock.wait();

                }
                // value is now true

                globalFlag = false;
                matrixCursor.addRow(new Object[]{selection, globalValue});
                globalValue = "";
                return matrixCursor;


            }
        } catch ( InterruptedException x ) {
            Log.d(TAG,"interrupted while waiting");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }


//            if(coordinatorPortNo.equals(myNode.getPortNo()))
//            {
//                // query locally
//                return returnCursorFromName(selection, matrixCursor);
//            }
//            else
//            {
//                // Ask the coordinator the query
//
//                try {
//                    synchronized (valueLock) {
//                        while ( globalFlag != true ) {
//
//                            new clientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, coordinatorPortNo,
//                                    constructQueryObject(selection), Constants.DATA_QUERY_REQUEST);
//                            valueLock.wait();
//
//                        }
//                        // value is now true
//
//                        globalFlag = false;
//                        matrixCursor.addRow(new Object[]{selection, globalValue});
//                        globalValue = "";
//                        return matrixCursor;
//
//
//                    }
//                } catch ( InterruptedException x ) {
//                    Log.d(TAG,"interrupted while waiting");
//                }
//
//            }





        return matrixCursor;
    }

    private MatrixCursor handleAllDataQuery(MatrixCursor matrixCursor)
    {
        try {

            // First get the local content

            for(File file: getContext().getFilesDir().listFiles())
            {
                String fileName = file.getName();
                String fileValue = getFileContentFromName(fileName);
                matrixCursor.addRow(new Object[]{fileName, fileValue});
            }

            // Now get the content from other ports

            synchronized (valueLock)
            {
                while ( globalFlag != true ) {

                    for(String port: Constants.ALL_PORTS) // change this to dynamic ports
                    {
                        if(!port.equals(myNode.getPortNo()))
                        {
                            new clientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                                    port, constructAllDataQueryObject(), Constants.ALL_DATA_QUERY_REQUEST);

                        }
                    }

                    valueLock.wait();

                }

                // value is now true

                globalFlag = false;

                // Add the remote content
                for(String s: allDataQueryReplyList)
                {
                    String a[] = s.split(Constants.KEY_VALUE_SEPARATOR);
                    matrixCursor.addRow(new Object[]{a[0], a[1]});
                }

                allDataQueryReplyCount = 0;
                allDataQueryActivePorts = 4;
                allDataQueryReplyList.clear(); // reset the values

            }

        } catch ( InterruptedException x ) {
            Log.d(TAG,"interrupted while waiting");
        }

        return matrixCursor;
    }

    private String constructAllDataQueryObject()
    {
        JSONObject jsonObject = new JSONObject();

        try {
            jsonObject.put(Constants.STATUS, 5); // 5 stands for all data query request
            jsonObject.put(Constants.QUERY_ORIGIN_PORT, myNode.getPortNo()); // port no of query node

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return jsonObject.toString();
    }

    private String queryAllValues()
    {

        //Log.d(TAG,"replying all data query");
        StringBuilder allFileContent = new StringBuilder("");

        for(File file: getContext().getFilesDir().listFiles())
        {
            String fileName = file.getName();
            String fileValue = getFileContentFromName(fileName);
            allFileContent.append(fileName+Constants.KEY_VALUE_SEPARATOR+fileValue+Constants.TEXT_SEPARATOR);
        }

        String content = allFileContent.toString();

        if(content.isEmpty())
            content = Constants.TEXT_SEPARATOR; // Sending only text separator when no files are present
        return constructAllDataQueryReplyObject(content);


    }

    private String constructAllDataQueryReplyObject(String content)
    {
        JSONObject jsonObject = new JSONObject();

        try {
            jsonObject.put(Constants.STATUS, 6); // 6 stands for all data query reply
            jsonObject.put(Constants.ALL_DATA_QUERY_CONTENT, content); // query content

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return jsonObject.toString();
    }

    private void handleAllDataDelete()
    {

        // Inform everybody else to delete their local files and don't care for their reply
        for(String port: Constants.ALL_PORTS)
        {
            if(!port.equals(myNode.getPortNo()))
            {
                new clientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                        port, constructAllDataDeleteObject(), Constants.ALL_DATA_DELETE_REQUEST);
            }
            else
                deleteAllLocalFiles();
        }
    }

    private void deleteAllLocalFiles()
    {
        for(File file: getContext().getFilesDir().listFiles())
        {
            file.delete();
        }
    }

    private String constructAllDataDeleteObject()
    {
        JSONObject jsonObject = new JSONObject();

        try {
            jsonObject.put(Constants.STATUS, 7); // 7 stands for all data delete

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return jsonObject.toString();
    }

    private void handleOtherDeleteCases(String selection)
    {
        try {
            String hashedKey = genHash(selection);
            String coordinatorPortNo = getCoordinatorPortNo(hashedKey);

            ArrayList<String> successors = getSuccessorPorts(coordinatorPortNo);

            if(coordinatorPortNo.equals(myNode.getPortNo()))
            {
                getContext().getFileStreamPath(selection).delete();
                //coordinatorDeleteList.add(selection);
                coordinatorHashMap.remove(selection);
                deleteReplicaDirectlyInSuccessors(selection, coordinatorPortNo);
            }
            else // ask the coordinator and it's successors to delete
            {
                new clientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                        coordinatorPortNo, constructDataDeleteObject(selection, coordinatorPortNo, successors.get(0),
                                successors.get(1)), Constants.SINGLE_DATA_DELETE_REQUEST);
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

    }

    private String constructDataDeleteObject(String selection, String coordinatorPort, String firstSuccPort, String secondSuccPort)
    {
        JSONObject jsonObject = new JSONObject();

        try {
            jsonObject.put(Constants.STATUS, 8); // 8 stands for single data file delete
            jsonObject.put(Constants.KEY, selection);
            jsonObject.put(Constants.COORDINATOR_PORT, coordinatorPort);
            jsonObject.put(Constants.FIRST_REPLICA_PORT, firstSuccPort);
            jsonObject.put(Constants.SECOND_REPLICA_PORT, secondSuccPort);

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return jsonObject.toString();
    }

    private void deleteReplicaInSuccessors(String key)
     {
        int index=1;
        for(String successorPort: getSuccessorPorts(myNode.getPortNo()))
        {
            new clientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, successorPort,
                    constructDeleteReplicaObject(key, index), Constants.DELETE_REPLICA_REQUEST);
            index++;
        }
    }

    private String constructDeleteReplicaObject(String key, int index)
    {
        JSONObject jsonObject = new JSONObject();

        try {
            jsonObject.put(Constants.STATUS, 9);
            jsonObject.put(Constants.KEY, key);
            jsonObject.put(Constants.REPLICA_INDEX, index);

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return jsonObject.toString();
    }

    private String constructInsertReplicaReply(String key, String dataOriginPort)
    {
        JSONObject jsonObject = new JSONObject();

        try {
            jsonObject.put(Constants.STATUS, 10); // 10 for insert replica reply
            jsonObject.put(Constants.KEY, key);
            jsonObject.put(Constants.DATA_ORIGIN_PORT, dataOriginPort);

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return jsonObject.toString();
    }

    private String constructReplicaDoneObject(String key)
    {
        JSONObject jsonObject = new JSONObject();

        try {
            jsonObject.put(Constants.STATUS, 11); // 11 for insert replica completed
            jsonObject.put(Constants.KEY, key);

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return jsonObject.toString();
    }

    protected void handleReplicationDone()
    {
        Log.d(TAG,"Release the insert lock");

        synchronized ( insertLock ) {
            insertFlag = true;
            insertLock.notify();  // notifyAll() might be safer...
        }
    }

    private void saveReplicaDirectlyInSuccessors(String key, String value, String dataOriginPort, String coordinatorPortNo)
    {
        int index=1;
        for(String successorPort: getSuccessorPorts(coordinatorPortNo))
        {
            new clientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, successorPort,
                    constructDirectReplicaInsertObject(key, value, dataOriginPort, index), Constants.DIRECT_INSERT_REPLICA_REQUEST);
            index++;
        }
    }

    private String constructDirectReplicaInsertObject(String key, String value, String dataOriginPort, int index)
    {
        JSONObject jsonObject = new JSONObject();

        try {
            jsonObject.put(Constants.STATUS, 12); // 12 for direct replica insert
            jsonObject.put(Constants.DATA_ORIGIN_PORT, dataOriginPort);
            jsonObject.put(Constants.KEY, key);
            jsonObject.put(Constants.VALUE, value);
            jsonObject.put(Constants.REPLICA_INDEX, index);

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return jsonObject.toString();
    }

    private String constructDirectInsertReplicaReply(String key, String dataOriginPort)
    {
        JSONObject jsonObject = new JSONObject();

        try {
            jsonObject.put(Constants.STATUS, 13); // 13 for direct insert replica reply
            jsonObject.put(Constants.KEY, key);
            jsonObject.put(Constants.DATA_ORIGIN_PORT, dataOriginPort);

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return jsonObject.toString();
    }

    private void askForMissingKeys()
    {
        //missingKeysReplyCount = 0;
        //missingKeysActivePorts = 3;

        ArrayList<String> predecessors = getPredecessorPorts();
        ArrayList<String> successors = getSuccessorPorts(myNode.getPortNo());

        int index = 100; // 100 for predecessor, 101 for pre-pred, 102 for successor
        for(String port: predecessors) // Ask predecessors first
        {
            new clientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, port,
                    constructMissingKeysRequestObject(myNode.getPortNo(), index), Constants.ASK_MISSING_KEYS);
            index++;
        }

        // Now ask successor - successors[0]
        new clientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, successors.get(0),
                constructMissingKeysRequestObject(myNode.getPortNo(), index), Constants.ASK_MISSING_KEYS);
    }

    private ArrayList<String> getPredecessorPorts()
    {
        ArrayList<String> ports= new ArrayList<String>();

        for(int i=0;i<nodeList.size();i++)
        {
            if(nodeList.get(i).getPortNo().equals(myNode.getPortNo()))
            {
                if(i==0)
                {
                    ports.add(nodeList.get(nodeList.size()-1).getPortNo()); // Predecessor
                    ports.add(nodeList.get(nodeList.size()-2).getPortNo()); // Predecessors Predecessor
                }
                else if(i==1)
                {
                    ports.add(nodeList.get(0).getPortNo());
                    ports.add(nodeList.get(nodeList.size()-1).getPortNo());
                }
                else
                {
                    ports.add(nodeList.get(i-1).getPortNo());
                    ports.add(nodeList.get(i-2).getPortNo());
                }

                break;
            }
        }

        for(String s: ports)
            Log.d(TAG,"Predecessor ports:"+s);

        return ports;
    }

    private String constructMissingKeysRequestObject(String askingPort, int keyType)
    {
        JSONObject jsonObject = new JSONObject();

        try {
            jsonObject.put(Constants.STATUS, 14); // 14 for asking missing keys
            jsonObject.put(Constants.ASKING_PORT, askingPort);
            jsonObject.put(Constants.KEY_TYPE, keyType);

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return jsonObject.toString();
    }

    private String getCoordinatorKeys()
    {
        StringBuilder sb = new StringBuilder("");

        Iterator it = coordinatorHashMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            sb.append(pair.getKey()+Constants.KEY_VALUE_SEPARATOR+pair.getValue());
            sb.append(Constants.TEXT_SEPARATOR);
        }

        String s = sb.toString();

        return s;
    }

    private String constructMissingKeysReplyObject(int keyType, String keys)
    {
        JSONObject jsonObject = new JSONObject();

        try {
            jsonObject.put(Constants.STATUS, 15); // 15 for missing keys reply
            jsonObject.put(Constants.KEY_TYPE, keyType);
            jsonObject.put(Constants.KEY_CONTENT, keys);


        } catch (JSONException e) {
            e.printStackTrace();
        }
        return jsonObject.toString();
    }

    private String getSuccessor1Keys()
    {
        StringBuilder sb = new StringBuilder("");

        Iterator it = successor1HashMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            sb.append(pair.getKey()+Constants.KEY_VALUE_SEPARATOR+pair.getValue());
            sb.append(Constants.TEXT_SEPARATOR);
        }

        String s = sb.toString();

        return s;
    }

    private void deleteReplicaDirectlyInSuccessors(String key, String coordinatorPortNo)
    {
        int index = 1;
        for(String successorPort: getSuccessorPorts(coordinatorPortNo))
        {
            new clientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, successorPort,
                    constructDirectReplicaDeleteObject(key, index), Constants.DIRECT_DELETE_REPLICA_REQUEST);
            index++;
        }
    }

    private String constructDirectReplicaDeleteObject(String key, int index)
    {
        JSONObject jsonObject = new JSONObject();

        try {
            jsonObject.put(Constants.STATUS, 16); // 16 for direct replica delete
            jsonObject.put(Constants.KEY, key);
            jsonObject.put(Constants.REPLICA_INDEX, index);

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return jsonObject.toString();
    }

    private void parseInsertContent(String insertContent, int type)
    {
        // Play insert
        Log.d(TAG,"Insert content:"+insertContent);
        String[] keyvals = insertContent.split(Pattern.quote(Constants.TEXT_SEPARATOR));
        Log.d(TAG,"keyvals:"+keyvals+" keyvals size:"+keyvals.length);

        for(String keyval: keyvals)
        {
            Log.d(TAG,"Each keyval:"+keyval);
            if(!keyval.isEmpty())
            {
                String[] a = keyval.split(Pattern.quote(Constants.KEY_VALUE_SEPARATOR));
                if(a.length == 2)
                {
                    Log.d(TAG,"A[0]:"+a[0]+" A[1]:"+a[1]);
                    saveLocally(a[0], a[1]); // Because any type of key should be saved locally
                    if(type == 100)
                    {
                        //successor1HashMap.clear();
                        successor1HashMap.put(a[0],a[1]);
                    }
                    else if(type == 101)
                    {
                        //successor2HashMap.clear();
                        successor2HashMap.put(a[0],a[1]);
                    }
                    else if(type == 102)
                    {
                        //coordinatorHashMap.clear();
                        coordinatorHashMap.put(a[0],a[1]);
                    }
                }

            }
        }

    }

    private class clientTask extends AsyncTask<String, String, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            boolean connectionFailed = false;

            try {

                Socket socket = new Socket();

                socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{(byte) 10, (byte) 0, (byte) 2, (byte) 2}),
                        Integer.parseInt(msgs[0])));

                String msgToSend = msgs[1];

                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                out.println(msgToSend);

                Log.d(TAG, "clientTask to port" + msgToSend + " : " + msgs[0]);

                InputStream inputStream = socket.getInputStream();
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                String reply = bufferedReader.readLine();
                Log.d(TAG, "Reply status:" + reply);

                if(reply == null || reply.isEmpty())
                    connectionFailed = true;
                else if(msgs[2].equals(Constants.INSERT_REPLICA_REQUEST))
                    publishProgress(Constants.INSERT_REPLICA_REPLY, reply);
                else if(msgs[2].equals(Constants.DATA_QUERY_REQUEST))
                     handleDataQueryReply(reply);
                else if(msgs[2].equals(Constants.ALL_DATA_QUERY_REQUEST))
                    handleAllDataQueryReply(reply);
                else if(msgs[2].equals(Constants.DIRECT_INSERT_REPLICA_REQUEST))
                    publishProgress(Constants.DIRECT_INSERT_REPLICA_REPLY, reply);
                else if(msgs[2].equals(Constants.ASK_MISSING_KEYS))
                    publishProgress(Constants.ASK_MISSING_KEYS, reply);

                bufferedReader.close();

                out.close();
                socket.close();

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
                connectionFailed = true;
            } catch (SocketException e) {
                Log.e(TAG, "Socket Exception, " + msgs[0] + " has failed");
                connectionFailed = true;
                e.printStackTrace();
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
                connectionFailed = true;
                e.printStackTrace();
            } catch (Exception e) {
                Log.e(TAG, "General exception");
                connectionFailed = true;
                e.printStackTrace();
            }

            if(connectionFailed)
                publishProgress(Constants.FAILURE_CASES, msgs[0], msgs[1], msgs[2]); // Const, remotePortNo, msgToSend, caseType

            return null;
        }

        protected void onProgressUpdate(String... strings) {

            //Log.d(TAG, "Received:" + strings[0] + " My port:" + myPort);
            if(strings[0].equals(Constants.INSERT_REPLICA_REPLY))
            {
                Log.v(TAG,"Insert replica reply handling");
                handleInsertReplicaReply(strings[1]);
            } else  if(strings[0].equals(Constants.FAILURE_CASES))
            {
                Log.v(TAG,"Failure case handling");
                handleFailureCases(strings[1], strings[2], strings[3]);
            } else  if(strings[0].equals(Constants.DIRECT_INSERT_REPLICA_REPLY))
            {
                Log.v(TAG,"Direct insert replica reply handling");
                handleDirectInsertReplicaReply(strings[1]);
            } else  if(strings[0].equals(Constants.ASK_MISSING_KEYS))
            {
                Log.v(TAG,"Missing keys reply handling");
                handleMissingKeysReply(strings[1]);
            }

            return;
        }

        protected void handleInsertReplicaReply(String object)
        {
            try {

                JSONObject jsonObject = new JSONObject(object);
                String key = jsonObject.getString(Constants.KEY);
                String dataOriginPort = jsonObject.getString(Constants.DATA_ORIGIN_PORT);

                String keyPort = key+Constants.TEXT_SEPARATOR+dataOriginPort;
                int count = insertReplicaHashMap.get(keyPort);
                count++;
                insertReplicaHashMap.put(keyPort, count); // Increment the insert count on reply

                checkReplicationStatus(key, dataOriginPort);

            } catch (JSONException e) {
                e.printStackTrace();
            }
        }

        protected void checkReplicationStatus(String key, String dataOriginPort)
        {
            String keyPort = key+Constants.TEXT_SEPARATOR+dataOriginPort;

            int count = insertReplicaHashMap.get(keyPort);
            int liveCount = liveReplicaHashMap.get(keyPort);

            if(count == liveCount)
            {
                // clear the hashmap entry
                insertReplicaHashMap.remove(keyPort);
                liveReplicaHashMap.remove(keyPort);

                // Got replies from both replicas, send reply to data origin port

                if(dataOriginPort.equals(myNode.getPortNo())) // If the data origin port is my port, I need not use clientTask
                {
                    handleReplicationDone();
                }
                else
                {
                    new clientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, dataOriginPort,
                            constructReplicaDoneObject(key), Constants.REPLICATION_DONE);
                }

            }
        }

        protected void handleDataQueryReply(String object)
        {
            try {

                Log.d(TAG, "Data query reply:" + object);

                JSONObject jsonObject = new JSONObject(object);
                String value = jsonObject.getString(Constants.VALUE);

                synchronized ( valueLock ) {
                    globalFlag = true;
                    globalValue = value;
                    valueLock.notify();  // notifyAll() might be safer...
                }

            } catch (JSONException e) {
                e.printStackTrace();
            }
        }

        protected void handleAllDataQueryReply(String object)
        {
            try {

                Log.d(TAG, "All Data query reply:" + object);

                JSONObject jsonObject = new JSONObject(object);
                String content = jsonObject.getString(Constants.ALL_DATA_QUERY_CONTENT);

                String[] allFileContent = content.split(Constants.TEXT_SEPARATOR);

                for(String s: allFileContent)
                {
                    if(!s.isEmpty())
                        allDataQueryReplyList.add(s); // add each key value pair to the list
                }
                allDataQueryReplyCount++;

                checkAllDataQueryStatus();

            } catch (JSONException e) {
                e.printStackTrace();
            }
        }

        private void checkAllDataQueryStatus()
        {
            Log.d(TAG,"allDataQueryReplyCount:"+allDataQueryReplyCount+" allDataQueryActivePorts:"+allDataQueryActivePorts);
            if(allDataQueryReplyCount == allDataQueryActivePorts) // Received content from all active emulators
            {
                Log.d(TAG,"Removing all data query lock");
                synchronized ( valueLock ) {
                    globalFlag = true;
                    valueLock.notify();  // notifyAll() might be safer...
                }
            }
        }

        protected void handleFailureCases(String remotePortNo, String failedObject, String caseType)
        {

            try {

                if(caseType.equals(Constants.INSERT_FORWARD_REQUEST))
                {
                    // It means coordinator has failed
                    JSONObject jsonObject = new JSONObject(failedObject);
                    String key = jsonObject.getString(Constants.KEY);
                    String value = jsonObject.getString(Constants.VALUE);
                    String originPort = jsonObject.getString(Constants.DATA_ORIGIN_PORT);

                    String keyPort = key+Constants.TEXT_SEPARATOR+originPort;

                    directInsertHashMap.put(keyPort, 0); // reset this
                    //directInsertReplicaReplyCount = 0;
                    // remotePortNo in thus case is the coordinatorNo
                    saveReplicaDirectlyInSuccessors(key, value, originPort, remotePortNo);
                }
                else if(caseType.equals(Constants.INSERT_REPLICA_REQUEST))
                {
                    // It means either of successor has failed

                    JSONObject jsonObject = new JSONObject(failedObject);
                    String key = jsonObject.getString(Constants.KEY);
                    String originPort = jsonObject.getString(Constants.DATA_ORIGIN_PORT);
                    int index = jsonObject.getInt(Constants.REPLICA_INDEX);

                    String keyPort = key+Constants.TEXT_SEPARATOR+originPort;

                    if(index == 1)
                    {
                        // First successor failed
                        int liveCount = liveReplicaHashMap.get(keyPort);
                        liveCount--;
                        liveReplicaHashMap.put(keyPort, liveCount); // decrement the count on failure

                    }
                    else if(index == 2)
                    {
                        // second successor failed
                        int liveCount = liveReplicaHashMap.get(keyPort);
                        liveCount--;
                        liveReplicaHashMap.put(keyPort, liveCount);
                    }

                    // On every failure, you got to check replication status
                    checkReplicationStatus(key, originPort);
                }
                else if(caseType.equals(Constants.DATA_QUERY_REQUEST))
                {
                    // It means tail of chain failed

                    JSONObject jsonObject = new JSONObject(failedObject);
                    String selection = jsonObject.getString(Constants.KEY);
                    String failedPort = jsonObject.getString(Constants.TAIL_PORT);
                    String secondReplicaPort = jsonObject.getString(Constants.SECOND_REPLICA_PORT);
                    String firstReplicaPort = jsonObject.getString(Constants.FIRST_REPLICA_PORT);
                    String coordinatorPort = jsonObject.getString(Constants.COORDINATOR_PORT);

                   if(failedPort.equals(secondReplicaPort))
                   {
                       // Tail is second succ and it has failed, so ask first succ and make it tail
                       new clientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, firstReplicaPort,
                               constructQueryObject(selection, coordinatorPort, firstReplicaPort, secondReplicaPort, firstReplicaPort),
                               Constants.DATA_QUERY_REQUEST);

                   }
                   else if(failedPort.equals(firstReplicaPort))
                   {
                       // Since we always query second succ first
                       // if failed port is first succ then both first and second succ have failed, so ask coordinator
                       new clientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, coordinatorPort,
                               constructQueryObject(selection, coordinatorPort, firstReplicaPort, secondReplicaPort, coordinatorPort),
                               Constants.DATA_QUERY_REQUEST);

                   }
                   else if(failedPort.equals(coordinatorPort))
                   {
                       // Both succ have failed and so does coordinator..
                       // No option but check if any of these 3 ports are online
                   }
                }
                else if(caseType.equals(Constants.ALL_DATA_QUERY_REQUEST))
                {
                    // Remote port has failed
                    allDataQueryActivePorts--;
                    checkAllDataQueryStatus();
                }
                else if(caseType.equals(Constants.SINGLE_DATA_DELETE_REQUEST))
                {
                    // Coordinator is not online, delete replicas directly

                    JSONObject jsonObject = new JSONObject(failedObject);
                    String key = jsonObject.getString(Constants.KEY);

                    deleteReplicaDirectlyInSuccessors(key, remotePortNo);
                }
                else if(caseType.equals(Constants.ASK_MISSING_KEYS))
                {
                    // Remote port has failed
                    //missingKeysActivePorts--;
                    //checkMissingKeysReplyStatus();
                }

            } catch (JSONException e) {
                Log.e(TAG,"JSON Exception occurred");
            }


        }

        protected void handleDirectInsertReplicaReply(String object)
        {
            try {

                JSONObject jsonObject = new JSONObject(object);
                int status = jsonObject.getInt(Constants.STATUS);
                String key = jsonObject.getString(Constants.KEY);
                String originPort = jsonObject.getString(Constants.DATA_ORIGIN_PORT);

                String keyPort = key+Constants.TEXT_SEPARATOR+originPort;

                if(status == 13) // Double checking
                {
                    int count = directInsertHashMap.get(keyPort);
                    count++;
                    directInsertHashMap.put(keyPort, count);

                    if(count == 2) // Got replies from both replicas, so you can unblock insert method
                    {
                        directInsertHashMap.remove(keyPort);
                        handleReplicationDone();
                    }
                }

            } catch (JSONException e) {
                e.printStackTrace();
            }
        }

        protected void handleMissingKeysReply(String object)
        {
            try {

                JSONObject jsonObject = new JSONObject(object);
                int keyType = jsonObject.getInt(Constants.KEY_TYPE);
                String keyContent = jsonObject.getString(Constants.KEY_CONTENT);

                if(!keyContent.isEmpty())
                {
                    parseInsertContent(keyContent, keyType);

// For readability
//                if(keyType == 100)  // Got your successor1 keys
//                {
//
//                    if(keyContent.isEmpty()) // Will never be empty, bcoz of delete keys separator
//                    {
//                        // You did not miss anything, do nothing
//                    }
//                    else
//                    {
//                        parseKeyContent(keyContent, 100);
//                    }
//                }
//                else if(keyType == 101) // Got your successor2 keys
//                {
//                    if(keyContent.isEmpty()) // Will never be empty, bcoz of delete keys separator
//                    {
//                        // You did not miss anything, do nothing
//                    }
//                    else
//                    {
//                        parseKeyContent(keyContent, 101);
//                    }
//                }
//                else if(keyType == 102) // Got your coordinator keys
//                {
//
//                    if(keyContent.isEmpty()) // Will never be empty, bcoz of delete keys separator
//                    {
//                        // You did not miss anything, do nothing
//                    }
//                    else
//                    {
//                        parseKeyContent(keyContent, 102);
//                    }
//                }
                }

                //missingKeysReplyCount++;
                //checkMissingKeysReplyStatus();

            } catch (JSONException e) {
                e.printStackTrace();
            }
        }

        private void checkMissingKeysReplyStatus()
        {
            Log.d(TAG,"missingKeysReplyCount:"+missingKeysReplyCount+" missingKeysActivePorts:"+missingKeysActivePorts);
            if(missingKeysReplyCount == missingKeysActivePorts) // Received content from all active emulators
            {
                Log.d(TAG,"Removing insert lock");
                synchronized ( insertLock ) {
                    insertFlag = true;
                    insertLock.notify();  // notifyAll() might be safer...
                }
            }
        }

    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            String message;
            Log.d(TAG, "Server task");

            while (true) {
                try {
                    Socket socket = serverSocket.accept();

                    InputStream inputStream = socket.getInputStream();
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                    message = bufferedReader.readLine();

                    Log.d(TAG,"Message received:"+message);
                    OutputStream outputStream = socket.getOutputStream();
                    BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream));

                    JSONObject jsonObject = new JSONObject(message);
                    int status = jsonObject.getInt(Constants.STATUS);
                    if (status == 1) {

                        Log.d(TAG,"Status is 1");
                        Log.v(TAG,"Insert forward request handling");
                        bufferedWriter.write(Constants.DUMMY_REPLY); // Send dummy replies to tell you are alive..
                        handleInsertForwardRequest(message);
                    } else if (status == 2) {

                        Log.d(TAG,"Status is 2");
                        Log.v(TAG,"Insert replica request handling");
                        String reply = handleInsertReplicaRequest(message);
                        bufferedWriter.write(reply);
                    } else if (status == 3) {

                        Log.d(TAG,"Status is 3");
                        String reply = handleDataQueryRequest(message);
                        bufferedWriter.write(reply);

                    } else if(status == 5){

                        Log.d(TAG,"Status is 5");
                        String reply = handleAllDataQueryRequest(message);
                        bufferedWriter.write(reply);
                    } else if(status == 7){

                        Log.d(TAG,"Status is 7");
                        bufferedWriter.write(Constants.DUMMY_REPLY); // Send dummy replies to tell you are alive..
                        //publishProgress(Constants.ALL_DATA_DELETE_REQUEST, message);
                        Log.v(TAG,"All Data delete request");
                        deleteAllLocalFiles();
                    } else if (status == 8) {

                        Log.d(TAG,"Status is 8");
                        bufferedWriter.write(Constants.DUMMY_REPLY); // Send dummy replies to tell you are alive..
                        //publishProgress(Constants.SINGLE_DATA_DELETE_REQUEST, message);
                        Log.v(TAG,"Single file delete request");
                        handleFileDeleteRequest(message);
                    } else if (status == 9) {

                        Log.d(TAG,"Status is 9");
                        bufferedWriter.write(Constants.DUMMY_REPLY); // Send dummy replies to tell you are alive..
                        //publishProgress(Constants.DELETE_REPLICA_REQUEST, message);
                        Log.v(TAG,"delete replica  request");
                        handleReplicaDeleteRequest(message);
                    } else if (status == 11) {

                        Log.d(TAG,"Status is 11");
                        bufferedWriter.write(Constants.DUMMY_REPLY); // Send dummy replies to tell you are alive..
                        //publishProgress(Constants.REPLICATION_DONE, message);
                        Log.v(TAG,"replication done");
                        handleReplicationDone();
                    } else if (status == 12) {

                        Log.d(TAG,"Status is 12");
                        String reply = handleDirectInsertReplicaRequest(message);
                        bufferedWriter.write(reply);
                    } else if (status == 14) {

                        Log.d(TAG,"Status is 14");
                        String reply = handleMissingKeysRequest(message);
                        bufferedWriter.write(reply);
                    } else if (status == 16) {

                        Log.d(TAG,"Status is 16");
                        bufferedWriter.write(Constants.DUMMY_REPLY); // Send dummy replies to tell you are alive..
                        //publishProgress(Constants.DELETE_REPLICA_REQUEST, message); // Direct delete or simple delete - both do the same job
                        Log.v(TAG,"direct delete replica  request");
                        handleReplicaDeleteRequest(message);
                    }


                    bufferedWriter.flush();
                    bufferedWriter.close();

                    bufferedReader.close();
                    socket.close();
                } catch (SocketTimeoutException e) {
                    Log.d(TAG, "Socket time out occurred");
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (JSONException e) {
                    e.printStackTrace();
                }
                //return null;
            }
        }


        protected void handleInsertForwardRequest(String object)
        {
            try {

                Log.d(TAG, "Insert forward request:" + object);

                JSONObject jsonObject = new JSONObject(object);
                String key = jsonObject.getString(Constants.KEY);
                String value = jsonObject.getString(Constants.VALUE);
                String originPort = jsonObject.getString(Constants.DATA_ORIGIN_PORT);

                saveLocally(key, value);
                String keyPort = key + Constants.TEXT_SEPARATOR + originPort;
                // Also tell next two successor to save the key, value pair
                insertReplicaHashMap.put(keyPort, 0);
                liveReplicaHashMap.put(keyPort, 2); // default 2 replicas
                coordinatorHashMap.put(key, value);
                saveReplicaInSuccessors(key, value, originPort, myNode.getPortNo());

            } catch (JSONException e) {
                e.printStackTrace();
            }
        }

        protected String handleInsertReplicaRequest(String object)
        {
            try {

                Log.d(TAG, "Insert replica request:" + object);

                JSONObject jsonObject = new JSONObject(object);
                String key = jsonObject.getString(Constants.KEY);
                String value = jsonObject.getString(Constants.VALUE);
                String originPort = jsonObject.getString(Constants.DATA_ORIGIN_PORT);
                int replicaIndex = jsonObject.getInt(Constants.REPLICA_INDEX);

                if(replicaIndex == 1)
                    successor1HashMap.put(key, value);
                else if(replicaIndex == 2)
                    successor2HashMap.put(key, value);

                 if(saveLocally(key, value))
                     return constructInsertReplicaReply(key, originPort);

            } catch (JSONException e) {
                e.printStackTrace();
            }
            return "";
        }

        protected String handleDataQueryRequest(String object)
        {
            try {

                Log.d(TAG, "Data query request:" + object);

                JSONObject jsonObject = new JSONObject(object);
                String key = jsonObject.getString(Constants.KEY);
                String value = getFileContentFromName(key);

                if(value == null || value.isEmpty())
                    return "";
                else
                    return constructQueryReplyObject(key, value);

            }
            catch (JSONException e) {
                e.printStackTrace();
            }

            return "";
        }

        protected String handleAllDataQueryRequest(String object)
        {
            Log.d(TAG, "All Data query request:" + object);

            return queryAllValues();
        }

        protected void handleFileDeleteRequest(String object)
        {
            try {

                Log.d(TAG, "File delete request:" + object);

                JSONObject jsonObject = new JSONObject(object);

                String selection = jsonObject.getString(Constants.KEY);

                getContext().getFileStreamPath(selection).delete();
                coordinatorHashMap.remove(selection);
                //coordinatorDeleteList.add(selection);

                // Now ask successors to delete this file
                deleteReplicaInSuccessors(selection);

            } catch (JSONException e) {
                e.printStackTrace();
            }

        }

        protected void handleReplicaDeleteRequest(String object)
        {
            try {

                Log.d(TAG, "File delete request:" + object);

                JSONObject jsonObject = new JSONObject(object);

                String selection = jsonObject.getString(Constants.KEY);
                int replicaIndex = jsonObject.getInt(Constants.REPLICA_INDEX);

                if(replicaIndex == 1)
                {
                    //successor1DeleteList.add(selection);
                    successor1HashMap.remove(selection);
                }
                else if(replicaIndex == 2)
                {
                    //successor2DeleteList.add(selection);
                    successor2HashMap.remove(selection);
                }

                getContext().getFileStreamPath(selection).delete();

            } catch (JSONException e) {
                e.printStackTrace();
            }

        }

        protected String handleDirectInsertReplicaRequest(String object)
        {
            try {

                Log.d(TAG, "Direct Insert replica request:" + object);

                JSONObject jsonObject = new JSONObject(object);
                String key = jsonObject.getString(Constants.KEY);
                String value = jsonObject.getString(Constants.VALUE);
                String originPort = jsonObject.getString(Constants.DATA_ORIGIN_PORT);
                int replicaIndex = jsonObject.getInt(Constants.REPLICA_INDEX);

                if(replicaIndex == 1)
                {
                    successor1HashMap.put(key, value);
                }

                else if(replicaIndex == 2)
                {
                    successor2HashMap.put(key, value);
                }


                if(saveLocally(key, value))
                    return constructDirectInsertReplicaReply(key, originPort);

            } catch (JSONException e) {
                e.printStackTrace();
            }
            return "";
        }

        protected String handleMissingKeysRequest(String object)
        {
            try {

                Log.d(TAG, "Missing keys request:" + object);

                JSONObject jsonObject = new JSONObject(object);
                String requestorPort = jsonObject.getString(Constants.ASKING_PORT);
                int keyType = jsonObject.getInt(Constants.KEY_TYPE);


                if(keyType == 100)
                {
                    // Requestor port is asking you for its successor1 keys - Asking for your coordinator keys
                    return constructMissingKeysReplyObject(100, getCoordinatorKeys());
                }
                else if(keyType == 101)
                {
                    // Requestor port is asking you for its successor2 keys - Asking for your coordinator keys
                    return constructMissingKeysReplyObject(101, getCoordinatorKeys());
                }
                else if(keyType == 102)
                {
                    // Requestor port is asking you for its coordinator keys - Asking for your successor1 keys
                    return constructMissingKeysReplyObject(102, getSuccessor1Keys());
                }


            } catch (JSONException e) {
                e.printStackTrace();
            }
            return "";
        }

    }

}
