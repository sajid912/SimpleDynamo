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
    private HashMap<String, Integer> insertReplicaHashMap = new HashMap<String, Integer>();

    ArrayList<String> coordinatorList = new ArrayList<String>();
    ArrayList<String> successor1List = new ArrayList<String>();
    ArrayList<String> successor2List = new ArrayList<String>();

    private int liveReplicaCount=2;

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

        Log.v("insert", values.toString());
        String key = values.getAsString("key");
        String value = values.getAsString("value");

        try {
            synchronized (insertLock) {
                while ( insertFlag != true ) {

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
                MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});

                for(File file: getContext().getFilesDir().listFiles())
                {
                    String fileName = file.getName();
                    String fileValue = getFileContentFromName(fileName);
                    matrixCursor.addRow(new Object[]{fileName, fileValue});
                }

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

        coordinatorList.clear();
        successor1List.clear();
        successor2List.clear();
        initializeNodeList();
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
            coordinatorList.add(key+Constants.KEY_VALUE_SEPARATOR+value);
            insertReplicaHashMap.put(key+Constants.TEXT_SEPARATOR+myNode.getPortNo(), 0);
            // Also tell next two successor to save the key, value pair
            saveReplicaInSuccessors(key, value, myNode.getPortNo());
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

    private String constructQueryObject(String key, String coordinatorPortNo)
    {
        JSONObject jsonObject = new JSONObject();

        try {
            jsonObject.put(Constants.STATUS, 3); // 3 status for data query request
            jsonObject.put(Constants.QUERY_ORIGIN_PORT, myNode.getPortNo());
            jsonObject.put(Constants.COORDINATOR_PORT, coordinatorPortNo);
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

    private void saveReplicaInSuccessors(String key, String value, String dataOriginPort)
    {
        int index=1;
        for(String successorPort: getSuccessorPorts(myNode.getPortNo()))
        {
            new clientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, successorPort,
                    constructInsertReplicaObject(key, value, dataOriginPort, index), Constants.INSERT_REPLICA_REQUEST);
            // 2 stands for insert replica request
            index++;
        }
    }

    private MatrixCursor handleOtherQueryCases(MatrixCursor matrixCursor, String selection)
    {
        try {

            String hashedKey = genHash(selection);
            String coordinatorPortNo = getCoordinatorPortNo(hashedKey);

            ArrayList<String> successorPorts = getSuccessorPorts(coordinatorPortNo);

            // chain replication - reads at tail, inserts at the head
            // Now ask the successors for query, starting with second successor first
            // In case second succ fails, ask the first succ

            String secondSuccessorPortNo = successorPorts.get(1); // Hardcoded bocz there are only two successors

            try {
                synchronized (valueLock) {
                    while ( globalFlag != true ) {

                        new clientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, secondSuccessorPortNo,
                                constructQueryObject(selection, coordinatorPortNo), Constants.DATA_QUERY_REQUEST);
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


        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }


        return matrixCursor;
    }

    private MatrixCursor returnCursorFromName(String fileName, MatrixCursor cursor)
    {
        String value = getFileContentFromName(fileName);
        cursor.addRow(new Object[]{fileName, value});
        return cursor;
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

            if(coordinatorPortNo.equals(myNode.getPortNo()))
            {
                getContext().getFileStreamPath(selection).delete();
            }
            else // ask the coordinator and it's successors to delete
            {
                new clientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                        coordinatorPortNo, constructDataDeleteObject(selection), Constants.SINGLE_DATA_DELETE_REQUEST);
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

    }

    private String constructDataDeleteObject(String selection)
    {
        JSONObject jsonObject = new JSONObject();

        try {
            jsonObject.put(Constants.STATUS, 8); // 8 stands for single data file delete
            jsonObject.put(Constants.KEY, selection);

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return jsonObject.toString();
    }

    private void deleteReplicaInSuccessors(String key)
     {

        for(String successorPort: getSuccessorPorts(myNode.getPortNo()))
        {
            new clientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, successorPort,
                    constructDeleteReplicaObject(key), Constants.DELETE_REPLICA_REQUEST);
        }
    }

    private String constructDeleteReplicaObject(String key)
    {
        JSONObject jsonObject = new JSONObject();

        try {
            jsonObject.put(Constants.STATUS, 9);
            jsonObject.put(Constants.KEY, key);

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

    private String stripVersion(String fileName)
    {
        return fileName.split(Constants.VERSION_SEPARATOR)[0];
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

                if(msgs[2].equals(Constants.INSERT_REPLICA_REQUEST) || msgs[2].equals(Constants.DATA_QUERY_REQUEST)
                        || msgs[2].equals(Constants.ALL_DATA_QUERY_REQUEST))
                {
                    InputStream inputStream = socket.getInputStream();
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                    String reply = bufferedReader.readLine();
                    Log.d(TAG, "Reply status:" + reply);
                    if(msgs[2].equals(Constants.INSERT_REPLICA_REQUEST))
                        publishProgress(Constants.INSERT_REPLICA_REPLY, reply);
                    else if(msgs[2].equals(Constants.DATA_QUERY_REQUEST))
                        handleDataQueryReply(reply);
                    else if(msgs[2].equals(Constants.ALL_DATA_QUERY_REQUEST))
                        handleAllDataQueryReply(reply);
                    bufferedReader.close();
                }

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
                publishProgress(Constants.FAILURE_CASES, msgs[1], msgs[2]);

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
                handleFailureCases(strings[1], strings[2]);
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
                insertReplicaHashMap.put(keyPort, count);

                if(count == liveReplicaCount)
                {
                    // clear the hashmap entry
                    insertReplicaHashMap.remove(keyPort);

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

            } catch (JSONException e) {
                e.printStackTrace();
            }
        }

        protected void checkReplicationStatus()
        {

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

                if(allDataQueryReplyCount == 4) // Received content from all active emulators
                {
                    synchronized ( valueLock ) {
                        globalFlag = true;
                        valueLock.notifyAll();  // notifyAll() might be safer...
                    }
                }


            } catch (JSONException e) {
                e.printStackTrace();
            }
        }

        protected void handleFailureCases(String failedObject, String caseType)
        {
            if(caseType.equals(Constants.INSERT_FORWARD_REQUEST))
            {
                // It means coordinator has failed
            }
            else if(caseType.equals(Constants.INSERT_REPLICA_REQUEST))
            {
                // It means either of successor has failed
                try {

                    JSONObject jsonObject = new JSONObject(failedObject);
                    int index = jsonObject.getInt(Constants.REPLICA_INDEX);
                    if(index == 1)
                    {
                        // First successor failed
                    }
                    else if(index == 2)
                    {
                        // second successor failed
                    }
                } catch (JSONException e) {
                    Log.e(TAG,"JSON Exception occurred");
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
                        publishProgress(Constants.INSERT_FORWARD_REQUEST, message);

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
                        publishProgress(Constants.ALL_DATA_DELETE_REQUEST, message);
                    } else if (status == 8) {

                        Log.d(TAG,"Status is 8");
                        publishProgress(Constants.SINGLE_DATA_DELETE_REQUEST, message);
                    } else if (status == 9) {

                        Log.d(TAG,"Status is 9");
                        publishProgress(Constants.DELETE_REPLICA_REQUEST, message);
                    } else if (status == 11) {

                        Log.d(TAG,"Status is 11");
                        publishProgress(Constants.REPLICATION_DONE, message);
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

        protected void onProgressUpdate(String... strings) {

            //Log.d(TAG, "Received:" + strings[0] + " My port:" + myPort);
            if(strings[0].equals(Constants.INSERT_FORWARD_REQUEST))
            {
                Log.v(TAG,"Insert forward request handling");
                handleInsertForwardRequest(strings[1]);
            }
            else  if(strings[0].equals(Constants.INSERT_REPLICA_REQUEST))
            {
              // Doesn't happen
            }
            else if(strings[0].equals(Constants.ALL_DATA_DELETE_REQUEST))
            {
                Log.v(TAG,"All Data delete request");
                deleteAllLocalFiles();
            }
            else if(strings[0].equals(Constants.SINGLE_DATA_DELETE_REQUEST))
            {
                Log.v(TAG,"Single file delete request");
                handleFileDeleteRequest(strings[1]);
            }
            else if(strings[0].equals(Constants.DELETE_REPLICA_REQUEST))
            {
                Log.v(TAG,"delete replica  request");
                handleReplicaDeleteRequest(strings[1]);
            }
            else if(strings[0].equals(Constants.REPLICATION_DONE))
            {
                Log.v(TAG,"replication done");
                handleReplicationDone();
            }


            return;
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
                coordinatorList.add(key+Constants.KEY_VALUE_SEPARATOR+value);
                saveReplicaInSuccessors(key, value, originPort);

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
                    successor1List.add(key+Constants.KEY_VALUE_SEPARATOR+value);
                else if(replicaIndex == 2)
                    successor2List.add(key+Constants.KEY_VALUE_SEPARATOR+value);

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

                return constructQueryReplyObject(key, getFileContentFromName(key));

            } catch (JSONException e) {
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

                getContext().getFileStreamPath(selection).delete();

            } catch (JSONException e) {
                e.printStackTrace();
            }

        }

    }

}
