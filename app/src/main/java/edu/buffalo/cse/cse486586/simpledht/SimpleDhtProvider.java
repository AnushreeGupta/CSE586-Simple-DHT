package edu.buffalo.cse.cse486586.simpledht;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;


import static android.R.attr.key;
import static android.R.attr.type;
import static android.R.attr.value;
import static android.R.id.message;
import static android.content.ContentValues.TAG;
import static edu.buffalo.cse.cse486586.simpledht.SQLHelper.TABLE_NAME;

/* ---------------------------------------REFERENCES----------------------------------------------:

* https://developer.android.com/reference/android/database/sqlite/SQLiteDatabase.html
* http://stackoverflow.com/questions/10723770/whats-the-best-way-to-iterate-an-android-cursor
* http://stackoverflow.com/questions/289434/how-to-make-a-java-thread-wait-for-another-threads-output
* http://stackoverflow.com/questions/9917935/adding-rows-into-cursor-manually
*
* -----------------------------------------------------------------------------------------------*/

public class SimpleDhtProvider extends ContentProvider {

    //static final String[] RemotePorts = { "11108", "11112", "11116", "11120", "11124"};
    static final int SERVER_PORT = 10000;

    final int initiatorPort = 5554;                          // Send Port i.e. first port
    String myPortID, successorID, predecessorID;             // All the port IDs associated with a node

    SQLHelper sqlhelper;                                    // SQL Helper class for using database as storage
    SQLiteDatabase mydatabase;                              // Database to store all the key-value pairs

    ArrayList<NodeMessage> chordRing = new ArrayList<NodeMessage>();  // Current active nodes in the ring

    String queryResult = "";                                // To get value for unique key query

    String globalDumpKeys = "";                             // To get all keys for global query
    String globalDumpValues = "";                           // To get all values for global query
    Boolean gotGlobalDump = false;                          // Flag to check if all results for global query are received

    String foundDeleteKey = "";

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        //---------------Implementation using SQLite Data Storage Method----------------//

        /* Handling for "@" symbol i.e. local delete from the current AVD */

        if(selection.equals("@")) {

            // For Local Delete from current Node
            int deleteResult = mydatabase.delete(sqlhelper.TABLE_NAME, null, null);
            //Log.d("Deleting @ at ", myPortID);

            return deleteResult;

        } else if(selection.equals("*")) {

            // For Global Delete from current Node
            int deleteResult = mydatabase.delete(sqlhelper.TABLE_NAME, null, null);

            NodeMessage node = new NodeMessage(myPortID, null, predecessorID, successorID, "DELETE_ALL");
            node.key = selection;
            node.originator = myPortID;
            String message = node.formMessage();

            Log.d("DELETE ALL Request", message);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
            Log.d("PROPAGATE DELETE to ::", successorID);

            return deleteResult;

        } else {

            /* Handling to find and delete the unique key from AVD it is present on  */

            String keyValue = sqlhelper.COLUMN_1 +" ='"+selection+"'";

            int deleteResult = mydatabase.delete(sqlhelper.TABLE_NAME, keyValue, null);
            //Log.d("DELETE Handler::", "BEFORE RESULT check");

            if (deleteResult > 0) {
                //Log.d(" DELETE RESULT ::", Integer.toString(deleteResult));
                return deleteResult;

            } else {
                NodeMessage node = new NodeMessage(myPortID, null, predecessorID, successorID, "DELETE_KEY");
                node.key = selection;
                node.originator = myPortID;
                String message = node.formMessage();

                //Log.d("Sending QUERY Request", message);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
                //Log.d("PROPAGATE QUERY to ::", successorID);

                return 0;
            }
        }
    }

    public void deleteHandler(String selection, String originator){

        /* Handler to lookup the key on the current AVD and if not found then propogate to the next one */

        Log.d("DELETE - Selection ", selection);

        String keyValue = sqlhelper.COLUMN_1 +" ='"+selection+"'";
        int deleteResult = mydatabase.delete(sqlhelper.TABLE_NAME, keyValue, null);

        Log.d("QUERY Handler::", "Before CURSOR check");
        NodeMessage node = new NodeMessage(myPortID, null, predecessorID, successorID, "DELETE_FOUND");
        node.key = selection;
        node.originator = originator;
        node.value = Integer.toString(deleteResult);

        if(deleteResult > 0 || successorID.equals(node.originator)) {

            Log.d(" DELETE RESULT ::", Integer.toString(deleteResult));
            String message = node.formMessage();

            Log.d("FOUND KEY", message);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
            Log.d("SEND CONFIRMATION to ::", originator);

        } else {

            node.msgType = "DELETE_KEY";
            String message = node.formMessage();
            Log.d("Sending DELETE Request", message);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
            Log.d("PROPAGATE DELETE to ::", successorID);

        }
    }

    public int deleteAllHandler(String originator){

        Log.d("DELETE - ALL ", originator);

        int deleteResult = mydatabase.delete(sqlhelper.TABLE_NAME, null, null);

        if(successorID.equals(originator)) {
            return deleteResult;

        } else {

            NodeMessage node = new NodeMessage(myPortID, null, predecessorID, successorID, "DELETE_ALL");
            node.originator = originator;
            node.value = Integer.toString(deleteResult);
            node.msgType = "DELETE_ALL";
            String message = node.formMessage();
            Log.d("Sending DELETE Request", message);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
            Log.d("PROPAGATE DELETE to ::", successorID);

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

        //---------------Implementation using SQLite Data Storage Method----------------//

        try {

            if(successorID.equals(myPortID) && predecessorID.equals(myPortID))
            {
                /* Case where only one AVD is present in the ring */

                Log.d("Condition 1", values.getAsString("key") +" " +values.getAsString("value"));
                mydatabase.insert(sqlhelper.TABLE_NAME, null, values);
                Log.d(TAG, "Insert Done");

            } else {

                /* Case where multiple AVDs can be present in the ring */

                Log.d(TAG, "Inserting : " + values.getAsString("key") + " " + myPortID + " " + successorID);
                insertHandler(values);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        Log.v("insert", values.toString());
        return uri;
    }

    public void insertHandler(ContentValues values){
        try {

            if(successorID.equals(myPortID) && predecessorID.equals(myPortID))
            {
                Log.d("CONDITION 1 ", values.getAsString("key") +" " +values.getAsString("value"));
                mydatabase.insert(sqlhelper.TABLE_NAME, null, values);
                Log.d(TAG, "Insert Done");

            } else {

                Log.d("Insert", "More than 1 AVDs");

                Log.d(TAG, "Inserting Propagate :: " + values.getAsString("key") + " " + myPortID + " " + successorID);
                String key = values.getAsString("key");
                String hashKey = genHash(key);
                String hashMyPort = genHash(myPortID);
                String hashSuccessor = genHash(successorID);
                String hashPredecessor = genHash(predecessorID);

                /* Checks on the hash of key to verify if new key lies within the partition of current port */

                int check1 = hashKey.compareTo(hashPredecessor);
                int check2 = hashKey.compareTo(hashMyPort);
                int check3 = hashPredecessor.compareTo(hashMyPort);
                int check4 = hashSuccessor.compareTo(hashMyPort);

                Log.d(TAG,"************************************");
                Log.d(TAG,""+ "KEY :: " + hashKey + "  MY PORT :: " + hashMyPort + "  PREDECESSOR :: "+ hashPredecessor + "  SUCCESSOR :: "+ hashSuccessor);
                Log.d(TAG," check3 > 0 ::"+(check3 > 0));
                Log.d(TAG," check1 < 0 && check2 < 0 :: "+(check1 < 0 && check2 < 0));
                Log.d(TAG," check1 > 0 && check2 > 0 :: "+(check1 > 0 && check2 > 0));
                Log.d(TAG," check1 > 0 && check2 < 0 :: "+(check1 > 0 && check2 < 0));

                Log.d(TAG,"************************************");

                if(check3 > 0) {

                    // Edge cases : Predecessor is greater than current

                    Log.d(TAG, "---------------Entering EDGE CASE Conditions-----------");

                    if(check1 < 0 && check2 < 0){

                        Log.d(TAG, "Entering CONDITION 2 :: " + values.getAsString("key") + " " + values.getAsString("value"));

                        long rowID = mydatabase.insert(sqlhelper.TABLE_NAME, null, values);

                        Log.d("CONDITION 2 ::", "Insert Done");

                    } else if(check1 > 0 && check2 > 0){

                        Log.d(TAG, "Entering CONDITION 3 :: " + values.getAsString("key") + " " + values.getAsString("value"));

                        long rowID = mydatabase.insert(sqlhelper.TABLE_NAME, null, values);

                        Log.d("CONDITION 3 ::", "Insert Done");

                    } else{
                        // Pass the value to successor of the current node

                        Log.d("CONDITION 5 :: ", "Propogate to Successor Node");

                        try {
                            NodeMessage node = new NodeMessage(myPortID, genHash(myPortID), predecessorID, successorID, "INSERT");
                            node.key = values.getAsString("key");
                            node.value = values.getAsString("value");

                            String message = node.formMessage();
                            Log.d(TAG,"CONDITION 5: InsertHandler node data before propogating:"+ message);

                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);

                            Log.d("CONDITION 5 ::", "Going to Node : " + successorID);

                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                }  else if (check1 > 0 && check2 < 0) {

                    // Ideal Case : Key is within the current partition i.e, greater than my predecessor and less than current

                    Log.d(TAG, "Entering CONDITION 6 :: " + values.getAsString("key") + " " + values.getAsString("value"));

                    long rowID = mydatabase.insert(sqlhelper.TABLE_NAME, null, values);

                    Log.d("CONDITION 6 ::", "Insert Done");

                } else {

                    // Pass the value to successor of the current node

                    Log.d("CONDITION 7 :: ", "Propogate to Successor Node");

                    try {
                        NodeMessage node = new NodeMessage(myPortID, genHash(myPortID), predecessorID, successorID, "INSERT");
                        node.key = values.getAsString("key");
                        node.value = values.getAsString("value");

                        String message = node.formMessage();
                        Log.d(TAG,"CONDITION 7: InsertHandler node data before propogating:"+ message);

                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);

                        Log.d("CONDITION 7 ::", "Going to Node : " + successorID);

                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean onCreate() {

        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        /* All port information for the current AVD */
        myPortID = portStr;
        successorID = portStr;
        predecessorID = portStr;

        /* Data storage via SQLite for the current AVD */
        sqlhelper = new SQLHelper(getContext());
        mydatabase = sqlhelper.getWritableDatabase();


        try {

            /* Starting the server for the current AVD */
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        } catch (Exception e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }


        if(myPortID.equals(Integer.toString(initiatorPort))){

            /* Case where new AVD is 5554, it adds itself to the chord ring.*/
            try{
                NodeMessage newNode = new NodeMessage();
                newNode.portID = myPortID;
                newNode.successor = successorID;
                newNode.predecessor = predecessorID;
                newNode.hashID = genHash(myPortID);

                chordRing.add(newNode);

            } catch(Exception e){
                e.printStackTrace();
            }

        } else {

            /* Case where any node other than 5554 joins requests to join the chord ring */
            try{
                NodeMessage node = new NodeMessage(myPortID, genHash(myPortID), predecessorID, successorID, "JOIN");
                String message = node.formMessage();
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, myPortID);

            } catch(Exception e){
                e.printStackTrace();
            }
        }

        return false;

    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {

        //---------------Implementation using SQLite Data Storage Method----------------//

        //Log.d("QUERY - Selection ", selection);
        SQLiteDatabase queryDatabase = sqlhelper.getReadableDatabase();

        try{

            if(selection.equals("@")) {
                // For Local Dump from current Node

                String selectQuery = "SELECT  * FROM " + sqlhelper.TABLE_NAME;
                Cursor cursor = queryDatabase.rawQuery(selectQuery, null);
                //Log.d("Querying @ at ", myPortID);

                return cursor;

            } else if(selection.equals("*")) {
                // For Global Dump from all the Nodes

                String selectQuery = "SELECT  * FROM " + sqlhelper.TABLE_NAME;
                Cursor cursor = queryDatabase.rawQuery(selectQuery, null);
                Log.d("Querying * at ", myPortID);

                if(successorID.equals(myPortID) && predecessorID.equals(myPortID)){
                    return cursor;
                }

                /*Log.d(TAG, "************************************");
                Log.d("ORIGINATOR KEY SIZE :: ", Integer.toString(cursor.getCount()) + " PORT :: "+ myPortID);
                Log.d(TAG, "************************************");*/

                String newKeys = "";
                String newValues = "";

                if (cursor != null) {
                    cursor.moveToFirst();
                    int keyIndex = cursor.getColumnIndex("key");
                    int valueIndex = cursor.getColumnIndex("value");

                    while (!cursor.isAfterLast()) {
                        newKeys = newKeys + cursor.getString(keyIndex) + "~~";
                        newValues = newValues + cursor.getString(valueIndex) + "~~";
                        cursor.moveToNext();

                    }
                }

                NodeMessage node = new NodeMessage(myPortID, null, predecessorID, successorID, "GLOBAL_QUERY");
                node.originator = myPortID;
                node.key = newKeys;
                node.value = newValues;
                String message = node.formMessage();

                //Log.d("GLOBAL DUMP Request", message);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
                //Log.d("PROPAGATE REQUEST to ::", successorID);

                while(!gotGlobalDump){

                }

                //Log.v(TAG,"After the while loop in originator");

                String [] allKeys = globalDumpKeys.split("~~");
                String [] allValues = globalDumpValues.split("~~");

                //Log.v(TAG,"In Query of * returned values length: "+allKeys.length+" "+allValues.length);
                MatrixCursor globalCursor = new MatrixCursor(new String[] { "key", "value"});

                for(int i = 0; i < allKeys.length; i++){
                    globalCursor.addRow(new String[] { allKeys[i], allValues[i]});
                }

                globalDumpKeys = "";
                globalDumpValues = "";

                return globalCursor;

            } else {

                // For Unique Selection Keys from current Node

                String[] columns = {"key", "value"};
                String keyValue = sqlhelper.COLUMN_1 + "=?";

                Cursor cursor = queryDatabase.query(sqlhelper.TABLE_NAME, columns, keyValue, new String[]{selection}, null, null, null);

                //Log.d("QUERY Handler::", "Before CURSOR check");

                if(cursor != null && cursor.getCount() > 0) {
                    //Log.d(" QUERY", Integer.toString(cursor.getCount()));

                    if (cursor.moveToFirst())
                        //Log.d("CURSOR DATA :: ", cursor.getString(cursor.getColumnIndex("value")));

                    return cursor;

                } else {

                    NodeMessage node = new NodeMessage(myPortID, null, predecessorID, successorID, "UNIQUE_QUERY");
                    node.key = selection;
                    node.originator = myPortID;
                    String message = node.formMessage();

                    //Log.d("Sending QUERY Request", message);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
                    //Log.d("PROPAGATE QUERY to ::", successorID);

                    while(queryResult.equals("")){

                    }
                    MatrixCursor resultCursor = new MatrixCursor(new String[] { "key", "value"});
                    resultCursor.addRow(new String[] { selection, queryResult});
                    queryResult = "";

                    return resultCursor;
                }

            }

        }catch (Exception e) {
            e.printStackTrace();
        }

        //Log.v(TAG, "***************QUERY*************** " + selection);
        return null;
    }

    public void uniqueQueryHandler(String selection, String originator){

        //---------------Implementation using SQLite Data Storage Method----------------//

        Log.d("QUERY - Selection ", selection);
        String[] columns = {"key", "value"};
        SQLiteDatabase queryDatabase = sqlhelper.getReadableDatabase();

        String keyValue = sqlhelper.COLUMN_1 + "=?";
        Cursor cursor = queryDatabase.query(sqlhelper.TABLE_NAME, columns, keyValue, new String[]{selection}, null, null, null);

        Log.d("QUERY Handler::", "Before CURSOR check");


        if(cursor != null && cursor.getCount() > 0) {
            Log.d(" QUERY", Integer.toString(cursor.getCount()));

            if (cursor.moveToFirst()) // data?
                Log.d("CURSOR DATA :: ", cursor.getString(cursor.getColumnIndex("value")));

            NodeMessage node = new NodeMessage(myPortID, null, predecessorID, successorID, "VALUE_FOUND");
            node.key = selection;
            node.value = cursor.getString(cursor.getColumnIndex("value"));
            node.originator = originator;
            String message = node.formMessage();

            Log.d("Sending Found Request", message);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
            Log.d("PROPAGATE QUERY to ::", originator);

        } else {
            NodeMessage node = new NodeMessage(myPortID, null, predecessorID, successorID, "UNIQUE_QUERY");
            node.key = selection;
            node.originator = originator;
            String message = node.formMessage();

            Log.d("Sending QUERY Request", message);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
            Log.d("PROPAGATE QUERY to ::", successorID);

        }

    }

    public void globalDumpHandler(String keysFetched, String valuesFetched, String originator){

        SQLiteDatabase queryDatabase = sqlhelper.getReadableDatabase();
        String selectQuery = "SELECT  * FROM " + sqlhelper.TABLE_NAME;
        Cursor myCursor = queryDatabase.rawQuery(selectQuery, null);

        String newKeys = "";
        String newValues = "";

        if (myCursor != null) {
            myCursor.moveToFirst();
            int keyIndex = myCursor.getColumnIndex("key");
            int valueIndex = myCursor.getColumnIndex("value");

            while (!myCursor.isAfterLast()) {
                newKeys = newKeys + myCursor.getString(keyIndex) + "~~";
                newValues = newValues + myCursor.getString(valueIndex) + "~~";
                myCursor.moveToNext();

            }
        }

        NodeMessage node = new NodeMessage(myPortID, null, predecessorID, successorID, "GLOBAL_FOUND");
        node.originator = originator;
        if(!keysFetched.equals("") && !valuesFetched.equals("")){
            node.key = keysFetched.concat(newKeys);
            node.value = valuesFetched.concat(newValues);

            Log.d("CASE 1 ::", node.key);
        } else {
            node.key = newKeys;
            node.value = newValues;

            Log.d("CASE 2 ::", node.key);
        }

        if(!successorID.equals(originator)) {

            node.msgType = "GLOBAL_QUERY";
            String message = node.formMessage();

            /*Log.d(TAG, "************************************************************************");
            Log.d("KEY SIZE :: ", Integer.toString(keysFetched.length()) + " PORT :: "+ myPortID);
            Log.d("NEW KEY SIZE :: ", Integer.toString(node.key.length()) + " PORT :: "+ myPortID);
            Log.d(TAG, "************************************************************************");*/

            //Log.d("GLOBAL DUMP Request", message);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
            //Log.d("PROPAGATE REQUEST to ::", successorID);

        } else {

            String message = node.formMessage();

            /*Log.d(TAG, "************************************************************************");
            Log.d("KEY SIZE :: ", Integer.toString(keysFetched.length()) + " PORT :: "+ myPortID);
            Log.d("NEW KEY SIZE :: ", Integer.toString(node.key.length()) + " PORT :: "+ myPortID);
            Log.d(TAG, "************************************************************************");

            Log.d("FOUND GLOBAL DUMP", message);*/

            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
            //Log.d("PROPAGATE RESULTS to ::", originator);


        }

    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
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

    private void sendMessageViaClient(String message){
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
    }

    /***
     * ServerTask is an AsyncTask that should handle incoming messages. It is created by
     * ServerTask.executeOnExecutor() call in SimpleMessengerActivity.
     * <p>
     * Please make sure you understand how AsyncTask works by reading
     * http://developer.android.com/reference/android/os/AsyncTask.html
     *
     * @author anushree
     */

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            //Log.d("I am in my Server ", myPortID);

            try {
                while (true) {
                    Socket clientSocket = serverSocket.accept();
                    Log.d("Server", "Accept Socket");

                    DataInputStream din = new DataInputStream(clientSocket.getInputStream());
                    DataOutputStream dout = new DataOutputStream(clientSocket.getOutputStream());
                    String input1 = "";
                    NodeMessage toSend = new NodeMessage();

                    // Failure Handling for incoming initial input message

                    try {

                        input1 = din.readUTF();

                        Log.d("SERVER ::", "Reading Input " +input1);
                        toSend.breakMessage(input1);

                        dout.writeUTF("MESSAGE_READ");
                        Log.d("Server Msg", input1);

                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    if(toSend.msgType.equals("JOIN")){

                        Log.d(TAG,"***************JOIN Request**************");

                        chordRing.add(toSend);
                        Collections.sort(chordRing);

                        Log.d("Chord Ring in Server", Integer.toString(chordRing.size()));

                        for(int index = 0; index < chordRing.size(); index++){
                            NodeMessage node = chordRing.get(index);

                            if(index == 0){
                                node.predecessor = chordRing.get(chordRing.size() - 1).portID;
                                node.successor = chordRing.get(index + 1).portID;
                                node.msgType = "UPDATE";

                            }else if (index == chordRing.size() - 1){
                                node.predecessor = chordRing.get(index - 1).portID;
                                node.successor = chordRing.get(0).portID;
                                node.msgType = "UPDATE";

                            } else {
                                node.predecessor = chordRing.get(index - 1).portID;
                                node.successor = chordRing.get(index + 1).portID;
                                node.msgType = "UPDATE";
                            }

                            chordRing.remove(index);
                            chordRing.add(node);
                            Collections.sort(chordRing);

                            Log.d("Chord Ring after sort", chordRing.get(index).predecessor + " " + chordRing.get(index).portID + " " + chordRing.get(index).successor);
                        }

                        toSend.msgType = "UPDATE";
                        sendMessageViaClient(toSend.formMessage());

                    } else if(toSend.msgType.equals("UPDATE")){

                        //Log.d(myPortID, "*****************UPDATE Sockets********************");
                        successorID = toSend.successor;
                        predecessorID = toSend.predecessor;
                        //Log.d("SERVER UPDATE:: ", myPortID + " " + successorID + " " + predecessorID);

                    } else if(toSend.msgType.equals("INSERT")){

                        //Log.d(myPortID, "*****************INSERT Message*********************");

                        ContentValues cValues = new ContentValues();
                        cValues.put("key", toSend.key);
                        cValues.put("value", toSend.value);

                        insertHandler(cValues);

                    } else if(toSend.msgType.equals("UNIQUE_QUERY")){

                        uniqueQueryHandler(toSend.key, toSend.originator);

                    } else if(toSend.msgType.equals("VALUE_FOUND")){

                        queryResult = toSend.value;

                    } else if(toSend.msgType.equals("GLOBAL_QUERY")){

                        /*Log.d(TAG, "************************************************************************");
                        Log.d("QUERY SERVER KEY :: ", Integer.toString(toSend.key.length()) + " PORT :: "+ myPortID);
                        Log.d("QUERY SERVER VALUE :: ", Integer.toString(toSend.value.length()) + " PORT :: "+ myPortID);
                        Log.d(TAG, "************************************************************************");
*/
                        globalDumpHandler(toSend.key, toSend.value, toSend.originator);

                    } else if(toSend.msgType.equals("GLOBAL_FOUND")){

                        /*Log.d(TAG, "************************************************************************");
                        Log.d("FOUND SERVER KEY :: ", Integer.toString(toSend.key.length()) + " PORT :: "+ myPortID);
                        Log.d("FOUND SERVER VALUE :: ", Integer.toString(toSend.value.length()) + " PORT :: "+ myPortID);
                        Log.d(TAG, "************************************************************************");
*/
                        if(toSend.key != null) {
                            globalDumpKeys = toSend.key;
                            globalDumpValues = toSend.value;
                        } else {
                            globalDumpKeys = "test";
                            globalDumpValues = "test";
                        }

                        gotGlobalDump = true;

                    } else if(toSend.msgType.equals("DELETE_KEY")){

                        /*Log.d(TAG, "************************************************************************");
                        Log.d("DELETE SERVER KEY :: ", toSend.key + " PORT :: "+ myPortID);
                        Log.d(TAG, "************************************************************************");
*/
                        deleteHandler(toSend.key, toSend.originator);

                    } else if(toSend.msgType.equals("DELETE_FOUND")){

                        /*Log.d(TAG, "************************************************************************");
                        Log.d("FOUND DELETE KEY :: ", toSend.key + " PORT :: "+ myPortID);
                        Log.d("FOUND DELETE VALUE :: ", toSend.value + " PORT :: "+ myPortID);
                        Log.d(TAG, "************************************************************************");
*/
                        foundDeleteKey = toSend.value;

                    } else if(toSend.msgType.equals("DELETE_ALL")){

                        /*Log.d(TAG, "************************************************************************");
                        Log.d("FOUND DELETE KEY :: ", toSend.key + " PORT :: "+ myPortID);
                        Log.d("FOUND DELETE VALUE :: ", toSend.value + " PORT :: "+ myPortID);
                        Log.d(TAG, "************************************************************************");
*/
                        deleteAllHandler(toSend.originator);
                    }

                    din.close();;
                    dout.close();
                }

            } catch(Exception e)
            {   e.printStackTrace();
                Log.e(TAG,"Server Socket Error");
            }
            return null;
        }

    }

    /***
     * ClientTask is an AsyncTask that should send a string over the network.
     * It is created by ClientTask.executeOnExecutor() call whenever OnKeyListener.onKey() detects
     * an enter key press event.
     *
     * @author anushree
     */
    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... data) {

            try {

                String msgToSend = data[0];
                Log.d(TAG, myPortID + " " + successorID + " " + predecessorID);

                if(msgToSend.contains("JOIN")) {
                    Log.v("In Client:","JOIN");

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt("11108"));
                    DataOutputStream dout = new DataOutputStream(socket.getOutputStream());

                    try{

                        dout.writeUTF(msgToSend);
                        dout.flush();

                        //Log.d("Message to 5554", msgToSend);
                    } catch(Exception e){
                        Log.d("***Client Messages***", msgToSend);
                    }

                    DataInputStream din = new DataInputStream(socket.getInputStream());
                    String ack = din.readUTF();
                    if(ack.equals("MESSAGE_READ")) {
                        dout.close();
                        din.close();
                        socket.close();
                    }

                } else if(msgToSend.contains("UPDATE")){

                    //Log.d("LOGS", "*********** MY PORT ID : " +myPortID+ " *****************");
                    //Log.d(TAG,"************************************************************************");

                    for(int i = 0; i < chordRing.size();i++){

                        if(chordRing.get(i).portID.equals("5554")) {
                            successorID = chordRing.get(i).successor;
                            predecessorID = chordRing.get(i).predecessor;

                            Log.d(TAG, chordRing.get(i).predecessor + " " + chordRing.get(i).portID + " " + chordRing.get(i).successor);

                        } else {

                            Log.d(TAG, chordRing.get(i).predecessor + " " + chordRing.get(i).portID + " " + chordRing.get(i).successor);
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(chordRing.get(i).portID) * 2);

                            DataOutputStream dout = new DataOutputStream(socket.getOutputStream());
                            dout.writeUTF(chordRing.get(i).formMessage());

                            DataInputStream din = new DataInputStream(socket.getInputStream());
                            String ack = din.readUTF();
                            if(ack.equals("MESSAGE_READ")) {
                                dout.close();
                                din.close();
                                socket.close();
                            }
                        }

                    }
                    //Log.d(TAG,"************************************************************************");

                } else if(msgToSend.contains("INSERT")) {
                    NodeMessage msgDetails = new NodeMessage();
                    msgDetails.breakMessage(msgToSend);

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgDetails.successor) * 2);
                    DataOutputStream dout = new DataOutputStream(socket.getOutputStream());
                    try{
                        dout.writeUTF(msgToSend);

                    } catch (Exception e){
                        e.printStackTrace();
                    }

                    DataInputStream din = new DataInputStream(socket.getInputStream());
                    String ack = din.readUTF();
                    if(ack.equals("MESSAGE_READ")) {
                        dout.close();
                        din.close();
                        socket.close();
                    }

                } else if(msgToSend.contains("UNIQUE_QUERY")){

                    NodeMessage msgQuery = new NodeMessage();
                    msgQuery.breakMessage(msgToSend);
                    Log.d("QUERY :: ", msgToSend);

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgQuery.successor) * 2);
                    DataOutputStream dout = new DataOutputStream(socket.getOutputStream());

                    try{
                        dout.writeUTF(msgToSend);

                    } catch (Exception e){
                        e.printStackTrace();;
                    }

                    DataInputStream din = new DataInputStream(socket.getInputStream());
                    String ack = din.readUTF();
                    if(ack.equals("MESSAGE_READ")) {
                        dout.close();
                        din.close();
                        socket.close();
                    }
                } else if(msgToSend.contains("VALUE_FOUND")){

                    NodeMessage msgQuery = new NodeMessage();
                    msgQuery.breakMessage(msgToSend);
                    Log.d("VALUE FOUND :: ", msgToSend);

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgQuery.originator) * 2);
                    DataOutputStream dout = new DataOutputStream(socket.getOutputStream());

                    try{
                        dout.writeUTF(msgToSend);

                    } catch (Exception e){
                        e.printStackTrace();;
                    }
                    DataInputStream din = new DataInputStream(socket.getInputStream());
                    String ack = din.readUTF();
                    if(ack.equals("MESSAGE_READ")) {
                        dout.close();
                        din.close();
                        socket.close();
                    }

                } else if(msgToSend.contains("GLOBAL_QUERY")){

                    NodeMessage msgQuery = new NodeMessage();
                    msgQuery.breakMessage(msgToSend);
                    //Log.d("GLOBAL QUERY :: ", msgToSend);

                    //Log.d(TAG, "************************************************************************");
                    //Log.d("QUERY CLIENT KEY :: ", Integer.toString(msgQuery.key.length()) + " PORT :: "+ myPortID+" Successor ID:"+msgQuery.successor);
                    //Log.d("QUERY CLIENT VALUE :: ", Integer.toString(msgQuery.value.length()) + " PORT :: "+ myPortID);
                    //Log.d(TAG, "************************************************************************");

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgQuery.successor) * 2);
                    DataOutputStream dout = new DataOutputStream(socket.getOutputStream());

                    try{
                        dout.writeUTF(msgToSend);
                        //Log.v(TAG,"Sent Global query message msgtosend:"+msgToSend);

                    } catch (Exception e){
                        e.printStackTrace();;
                    }

                    DataInputStream din = new DataInputStream(socket.getInputStream());
                    String ack = din.readUTF();
                    if(ack.equals("MESSAGE_READ")) {
                        dout.close();
                        din.close();
                        socket.close();
                    }

                }  else if(msgToSend.contains("GLOBAL_FOUND")){

                    NodeMessage msgQuery = new NodeMessage();
                    msgQuery.breakMessage(msgToSend);
                    //Log.d("GLOBAL FOUND :: ", msgToSend);

                    /*Log.d(TAG, "************************************************************************");
                    Log.d("FOUND CLIENT KEY :: ", Integer.toString(msgQuery.key.length()) + " PORT :: "+ myPortID+" Originator ID:"+msgQuery.originator);
                    Log.d("FOUND CLIENT VALUE :: ", Integer.toString(msgQuery.value.length()) + " PORT :: "+ myPortID);
                    Log.d(TAG, "************************************************************************");
*/

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgQuery.originator) * 2);
                    DataOutputStream dout = new DataOutputStream(socket.getOutputStream());

                    try{
                        dout.writeUTF(msgToSend);

                    } catch (Exception e){
                        e.printStackTrace();;
                    }

                    DataInputStream din = new DataInputStream(socket.getInputStream());
                    String ack = din.readUTF();
                    if(ack.equals("MESSAGE_READ")) {
                        dout.close();
                        din.close();
                        socket.close();
                    }

                } else if(msgToSend.contains("DELETE_KEY")){

                    NodeMessage msgQuery = new NodeMessage();
                    msgQuery.breakMessage(msgToSend);
                    //Log.d("DELETE_KEY :: ", msgToSend);

                    /*Log.d(TAG, "************************************************************************");
                    Log.d("DELETE CLIENT KEY :: ", Integer.toString(msgQuery.key.length()) + " PORT :: "+ myPortID+" Successor ID:"+msgQuery.successor);
                    Log.d("DELETE CLIENT VALUE :: ", Integer.toString(msgQuery.value.length()) + " PORT :: "+ myPortID);
                    Log.d(TAG, "************************************************************************");
*/
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgQuery.successor) * 2);
                    DataOutputStream dout = new DataOutputStream(socket.getOutputStream());

                    try{
                        dout.writeUTF(msgToSend);

                    } catch (Exception e){
                        e.printStackTrace();;
                    }

                    DataInputStream din = new DataInputStream(socket.getInputStream());
                    String ack = din.readUTF();
                    if(ack.equals("MESSAGE_READ")) {
                        dout.close();
                        din.close();
                        socket.close();
                    }

                } else if(msgToSend.contains("DELETE_FOUND")){

                    NodeMessage msgQuery = new NodeMessage();
                    msgQuery.breakMessage(msgToSend);
                    //Log.d("DELETE_FOUND :: ", msgToSend);

                    /*Log.d(TAG, "************************************************************************");
                    Log.d("FOUND DELETE KEY :: ", Integer.toString(msgQuery.key.length()) + " PORT :: "+ myPortID+" Originator ID:"+msgQuery.originator);
                    Log.d("FOUND DELETE VALUE :: ", Integer.toString(msgQuery.value.length()) + " PORT :: "+ myPortID);
                    Log.d(TAG, "************************************************************************");
*/
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgQuery.originator) * 2);
                    DataOutputStream dout = new DataOutputStream(socket.getOutputStream());

                    try{
                        dout.writeUTF(msgToSend);

                    } catch (Exception e){
                        e.printStackTrace();;
                    }

                    DataInputStream din = new DataInputStream(socket.getInputStream());
                    String ack = din.readUTF();
                    if(ack.equals("MESSAGE_READ")) {
                        dout.close();
                        din.close();
                        socket.close();
                    }

                } else if(msgToSend.contains("DELETE_ALL")){

                    NodeMessage msgQuery = new NodeMessage();
                    msgQuery.breakMessage(msgToSend);

                    /*Log.d(TAG, "************************************************************************");
                    Log.d("FOUND DELETE KEY :: ", Integer.toString(msgQuery.key.length()) + " PORT :: "+ myPortID+" Originator ID:"+msgQuery.originator);
                    Log.d("FOUND DELETE VALUE :: ", Integer.toString(msgQuery.value.length()) + " PORT :: "+ myPortID);
                    Log.d(TAG, "************************************************************************");
*/
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgQuery.successor) * 2);
                    DataOutputStream dout = new DataOutputStream(socket.getOutputStream());

                    try{
                        dout.writeUTF(msgToSend);

                    } catch (Exception e){
                        e.printStackTrace();;
                    }

                    DataInputStream din = new DataInputStream(socket.getInputStream());
                    String ack = din.readUTF();
                    if(ack.equals("MESSAGE_READ")) {
                        dout.close();
                        din.close();
                        socket.close();
                    }

                }

            } catch (Exception e) {
                Log.e(TAG, "ClientTask socket IOException");
            }

            return null;
        }
    }
}
