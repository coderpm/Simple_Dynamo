package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteException;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

    /**
     * Global variables
     */
    static final int SERVER_PORT = 10000;
    static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    static String myPort;       //Stores the using port for hashing purposes i.e. 5554 kind
    static String selfHash;     //Stores the hash value for your own string
    static String delimiter ="~!";      //Delimiter while sending message to client
    static ContentValues dhtContentValues;
    static Uri dhtUri;
    static ContentResolver dhtContentResolver;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";

    //For Dynamo
    String [] portArray = {"5554","5562","5556","5560","5558"};

    static HashMap<String,String> ringMap = new HashMap<String, String>();
    static HashMap<String,String> myMap = new HashMap<String,String>();
    static ArrayList<String> keyList = new ArrayList<String>();
    static ArrayList<String> modKeyList = new ArrayList<String>();

    static HashMap<String,HashMap<String, String>> finalRing = new HashMap<String, HashMap<String,String>>();
    static HashMap<String,String> portHash = new HashMap<String, String>();
    static HashMap<String,String> hashPort = new HashMap<String, String>();

    //static HashMap<String,String> portUpStatus = new HashMap<String,String>();
    static ArrayList<String> portUpStatus  = new ArrayList<String>();
    static boolean statusActive=false;
    static int ringCount =0;

    int timeOut=2200;

    static int insertMessageNumber=0;
    static int queryMessageNumber=0;


    static HashMap<String,String> insertAck = new HashMap<String, String>();
    static HashMap<String,String> queryAck = new HashMap<String, String>();


    //For * retrieve functions
    static int replyCount=0;
    static HashMap<String,String> AllReply = new HashMap<String, String>();


    //For recovery functions
    static HashMap<String,String> failureMap = new HashMap<String, String>();       //For saving the failures accounted by me
    static int failuniqueNum=0;             //For getting failure number for the failure to keep them unique

    //For my own recovery - to be filled by client methods with messageType as resurrection
    static HashMap<String,HashMap<String, String>> recoverData = new HashMap<String, HashMap<String,String>>();

    /**
     * Database specific constant declarations
     */

    public static SQLiteDatabase accessdb, readingDb, writingDb;
    static final String DB_NAME = "DYNAMO";
    static final String TABLE_NAME = "Message";
    static final String firstColumn = "key";
    static final String secondColumn = "value";
    static final String thirdColumn = "avd";
    static final String fourthColumn = "version";
    static final int DATABASE_VERSION = 1;

    static final String CREATE_DB_TABLE =
            " CREATE TABLE " + TABLE_NAME +
                    " (key TEXT PRIMARY KEY, " + " value TEXT NOT NULL," + " avd TEXT," + " version INT);";

    private static class accessDBHelp extends SQLiteOpenHelper {
        accessDBHelp(Context context) {
            super(context, DB_NAME, null, DATABASE_VERSION);
        }

        @Override
        public void onCreate(SQLiteDatabase db)
        {
            System.out.println("USER:::Creating table query " + CREATE_DB_TABLE);
            try {
                db.execSQL("DROP TABLE IF EXISTS " + TABLE_NAME);
                db.execSQL(CREATE_DB_TABLE);

            } catch (Exception e)
            {
                e.printStackTrace();
                Log.e(TAG, "Can't create table");
            }
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion,int newVersion)
        {
            db.execSQL("DROP TABLE IF EXISTS " + TABLE_NAME);
            onCreate(db);
        }
    }

    /**
     * End of Database Specific functions
     *
     */

    /**
     * Start of URI Builder
     */

    private Uri buildUri(String scheme, String authority)
    {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public boolean onCreate()
    {

        File dbFile = getContext().getDatabasePath(DB_NAME);

        if(dbFile.exists())
        {
            //Means it is the first time that the avd has come up
            System.out.println("Means first time AVD CAME UP");
            statusActive= false;
        }
        else
        {
            //Means it came back after getting down -- Call function to retrieve all the missed values

            statusActive= true;
        }


        //Initializing the DB variables
        Context con = getContext();
        accessDBHelp dbHelp = new accessDBHelp(getContext());

        SQLiteDatabase cdb = new accessDBHelp(getContext()).getWritableDatabase();
        writingDb = dbHelp.getWritableDatabase();
        readingDb= dbHelp.getReadableDatabase();

        //Get own port
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr)));
        portUpStatus.add(myPort);

        dhtUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
        dhtContentValues = new ContentValues();

        //Call a function to make the ring -- but activate the ring only after receiving message from others
        makeRing();


        //Now check if this is the first time or resurrection after failure

        Cursor reader=null;




        try{
            String getQuery = "SELECT * FROM "+ TABLE_NAME +";";
        System.out.println("USER:: The get query is "+getQuery);


            if(!(readingDb.isOpen()))
                System.out.println("Readingdb is closed");

            reader = readingDb.rawQuery(getQuery,null);


        }catch(SQLiteException e){
            e.printStackTrace();
        }catch(Exception e){
            e.printStackTrace();
        }

        //Make a server Socket
        try
        {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            System.out.println("USER::Server socket created for "+myPort);


            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        } catch (Exception e)
        {
            e.printStackTrace();
            Log.e(TAG, "Can't create a ServerSocket");
            return true;
        }


        if(statusActive)
        {
            //Means it is the first time that the avd has come up
            System.out.println("Means first time AVD CAME UP");
            statusActive= true;
        }
        else
        {
            //Means it came back after getting down -- Call function to retrieve all the missed values

            statusActive= false;
            System.out.println("Means AVD RECOVERED AFTER FAILURE");
            //Call function to back up the data
            recoveryfromAll();

        }







        //Returning default value
        return false;



    }
    void recoveryfromAll()
    {

        System.out.println("Trying to recover after failure");
        HashMap<String,String> temp =  finalRing.get(myPort);
        String successor1 = hashPort.get(temp.get("next1"));
        String successor2 = hashPort.get(temp.get("next2"));
        String prev1 = hashPort.get(temp.get("prev1"));
        String prev2 = hashPort.get(temp.get("prev2"));

        System.out.println("In failure :: Successor 1 is "+successor1);
        System.out.println("In failure :: Successor 2 is "+successor2);
        System.out.println("In failure :: Prev 1 is "+prev1);
        System.out.println("In failure :: Prev 2 is "+prev2);


        //Now call clientTask in order to retrieve all the data for successor1, successor2, prev1 and prev2

        String requestMessage = "resurrection"+delimiter+myPort+delimiter+successor1;
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,requestMessage,myPort);


/*
        requestMessage = "resurrection"+delimiter+myPort+delimiter+successor2;
        new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,requestMessage,myPort);
*/

        requestMessage = "resurrection"+delimiter+myPort+delimiter+prev1;
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,requestMessage,myPort);

        requestMessage = "resurrection"+delimiter+myPort+delimiter+prev2;
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,requestMessage,myPort);

        System.out.println("Now keep looping till data is received");

        try
        {
            while(true)
            {
                if(recoverData.size()==3)
                    break;
                else
                    Thread.sleep(100);
            }
        }catch (Exception e)
        {
            e.printStackTrace();
        }

        System.out.println("Now data has been received -- Update your own DB");

        String saveavd=null;
        //Iterate over the recoverData hashmap and retrieve the data
        for(String key: recoverData.keySet()) {
            System.out.println("The data is from "+key);
            if(key.equals(successor1)|| key.equals(successor2))
                saveavd=myPort;
            else
                saveavd=key;
                //Key is the avd number and value is another hashmap
            HashMap<String,String> tempMap = recoverData.get(key);
            for(String key1: tempMap.keySet())
            {
                System.out.println("WHILE RECOVERY:: Inserting key "+key1+ " and value "+tempMap.get(key1));
                Fast_DB_Insert(key1,tempMap.get(key1),saveavd);
            }
            //Now Iterate over this hashmap and perform operations
            //TODO:COMPLETE THE RECOVERY PROCESS

        }
        System.out.println("Recovery Process Complete");

        recoverData.clear();
        //After Update is complete
        statusActive=true;
    }


    public void makeRing()
    {
        String inputs[]={"5554","5562","5556","5560","5558"};
        String input=null;
        int counter;


        int checkCounter=0;


        try
        {
            //Store in hashmap--1
            input=myPort;
            keyList.add(genHash(input));
            ringMap.put(genHash(input), input);

            portHash.put(input, genHash(input));
            hashPort.put(genHash(input), input);

            for(counter=0;counter<inputs.length;counter++)
            {
                if(inputs[counter].equals(myPort))
                    continue;
                //Now new node joins
                input=inputs[counter];
                String newHash= genHash(input);
                ringMap.put(newHash, input);
                int finalindex=findIndex(newHash);
                keyList.add(finalindex, newHash);

                portHash.put(input, genHash(input));
                hashPort.put(genHash(input), input);

            }

            //make a new keylist of size 4 more than the original size with last two
            modKeyList.add(keyList.get(keyList.size()-2));
            modKeyList.add(keyList.get(keyList.size()-1));
            for(counter=0;counter<keyList.size();counter++)
                modKeyList.add(keyList.get(counter));

            modKeyList.add(keyList.get(keyList.size()-keyList.size()));
            modKeyList.add(keyList.get((keyList.size()-keyList.size())+1));

  //          System.out.println("Original Key list is ");
            for(counter=0;counter<keyList.size();counter++)
            {
                //Iterating the sorted list
    //            System.out.println("They port is is "+hashPort.get(keyList.get(counter))+ " and hash is "+keyList.get(counter));
            }

  //          System.out.println("Making MAP is ");
            for(counter=2;counter<modKeyList.size()-2;counter++)
            {
                HashMap<String, String> tempinside = new HashMap<String, String>();


                //Make hashmap of next1,next2,prev1,prev2
                tempinside.put("selfhash",modKeyList.get(counter));

                //Means the first element -- need to see prev1 and prev2
                tempinside.put("prev1",modKeyList.get(counter-1));
                tempinside.put("prev2",modKeyList.get(counter-2));

                //Save the next two
                tempinside.put("next1",modKeyList.get(counter+1));
                tempinside.put("next2",modKeyList.get(counter+2));

                finalRing.put(hashPort.get(modKeyList.get(counter)), tempinside);

            }
        } catch (NoSuchAlgorithmException e)
        {
            e.printStackTrace();
        }

    }

    static public int findIndex(String hashStr)
    {
        int counter=0,compareTo=0;
        int finalindex=0;
        for(counter=0;counter<keyList.size();counter++)
        {
            compareTo= keyList.get(counter).compareTo(hashStr);
            if(compareTo<0)
            {
                //Means allhash < newHash
                finalindex=counter+1;
            }
            else if(compareTo>0)
            {
                finalindex=counter;
                break;
            }

        }
        return finalindex;
    }


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        System.out.println("USER::This is inside delete function deleting:: "+selection);

        //Check if the the statusActive is true or not
        while(true)
        {
            if(statusActive==true)
            {
                break;
            }
            else
            {
                System.out.println("Sorry still backing up");
                //Means avd backup is going on
                //Thread.sleep(100);
            }
        }

        if(selection.equals("\"*\""))
        {
            // >> * >> Delete values from entire DHT
            System.out.println("Delete all the values");

            String requestMessage ="deleteAll";
            try
            {
                CreateMessage sendMessage = new CreateMessage(requestMessage);

                String deleteQuery = "SELECT * FROM " + TABLE_NAME +";";
                System.out.println("USER:: The delete query is " + deleteQuery);
                writingDb.delete(TABLE_NAME,null,null);

                for (String aPortArray : portArray) {

                    if(aPortArray.equals(myPort))
                        continue;
                    int port = Integer.parseInt(aPortArray);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (port * 2));
                    socket.setSoTimeout(timeOut);
                    ObjectOutputStream clientOut = new ObjectOutputStream(socket.getOutputStream());
                    clientOut.writeObject(sendMessage);

                    System.out.println("Message being sent to" + port);
                    //Close all the sockets and input and output stream
                    clientOut.close();
                    socket.close();

                }


            }catch (Exception e)
            {
                e.printStackTrace();
            }
        }
        else if(selection.equals("\"@\""))
        {
            try
            {
                String deleteQuery = "SELECT * FROM " + TABLE_NAME +";";
                System.out.println("USER:: The delete query is " + deleteQuery);
                writingDb.delete(TABLE_NAME,null,null);

            }catch(Exception e)
            {
                e.printStackTrace();
            }

        }
        else
        {
            String resultNode=findNode(selection);

            String requestMessage ="deleteAll";
            try
            {
                CreateMessage sendMessage = new CreateMessage(requestMessage);

                String deleteQuery = "SELECT * FROM " + TABLE_NAME +";";
                System.out.println("USER:: The delete query is " + deleteQuery);
                writingDb.delete(TABLE_NAME,null,null);

                for (String aPortArray : portArray) {

                    if(aPortArray.equals(myPort))
                        continue;
                    int port = Integer.parseInt(aPortArray);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (port * 2));
                    socket.setSoTimeout(timeOut);
                    ObjectOutputStream clientOut = new ObjectOutputStream(socket.getOutputStream());
                    clientOut.writeObject(sendMessage);

                    System.out.println("Message being sent to" + port);
                    //Close all the sockets and input and output stream
                    clientOut.close();
                    socket.close();

                }


            }catch (Exception e)
            {
                e.printStackTrace();
            }


            if(resultNode.equals(myPort))
            {
                //Delete from self and my successors
                System.out.println("Deleting one");
                String delQuery="DELETE FROM "+TABLE_NAME+" WHERE "+firstColumn+"= '"+selection+"';";
                System.out.println("The delete query is "+delQuery);
                writingDb.execSQL(delQuery);
                Log.v("delete done", selection);

                HashMap<String,String> temp =  finalRing.get(myPort);
                String successor1 = hashPort.get(temp.get("next1"));
                String successor2 = hashPort.get(temp.get("next2"));

                System.out.println("Successor 1 is "+successor1);
                System.out.println("Successor 2 is "+successor2);

                int successor1Port = Integer.parseInt(successor1);
                int successor2Port = Integer.parseInt(successor2);

                try
                {
                    //Make the message
                    CreateMessage sendMessage = new CreateMessage("deleteOne",selection);

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (successor1Port * 2));
                    //socket.setSoTimeout(timeOut);
                    ObjectOutputStream clientOut = new ObjectOutputStream(socket.getOutputStream());
                    clientOut.writeObject(sendMessage);

                    System.out.println("Message being sent to" + successor1Port);
                    //Close all the sockets and input and output stream
                    clientOut.close();
                    socket.close();

                    //Send to second successor
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (successor2Port * 2));
                    clientOut = new ObjectOutputStream(socket.getOutputStream());
                    clientOut.writeObject(sendMessage);

                    System.out.println("Message being sent to" + successor2Port);

                    //Close all the sockets and input and output stream
                    clientOut.close();
                    socket.close();

                }catch(Exception e)
                {
                    e.printStackTrace();
                }
            }
            else //key not in my DB
            {
                //Message to delete from coordinator and successors
                HashMap<String,String> temp =  finalRing.get(myPort);
                String successor1 = hashPort.get(temp.get("next1"));
                String successor2 = hashPort.get(temp.get("next2"));

                System.out.println("Successor 1 is "+successor1);
                System.out.println("Successor 2 is "+successor2);

                int coordinator = Integer.parseInt(resultNode);
                int successor1Port = Integer.parseInt(successor1);
                int successor2Port = Integer.parseInt(successor2);

                try
                {
                    CreateMessage sendMessage = new CreateMessage("deleteOne",selection);

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (coordinator * 2));
                    //socket.setSoTimeout(timeOut);
                    ObjectOutputStream clientOut = new ObjectOutputStream(socket.getOutputStream());
                    clientOut.writeObject(sendMessage);

                    System.out.println("Message being sent to" + coordinator);
                    //Close all the sockets and input and output stream
                    clientOut.close();
                    socket.close();

                }catch(Exception e)
                {
                    e.printStackTrace();
                }
                if(successor1.equals(myPort) || successor2.equals(myPort))
                {
                    if(successor1.equals(myPort))
                    {
                        System.out.println("Deleting one");
                        String delQuery="DELETE FROM "+TABLE_NAME+" WHERE "+firstColumn+"= '"+selection+"';";
                        System.out.println("The delete query is "+delQuery);
                        writingDb.execSQL(delQuery);
                        Log.v("delete done", selection);

                        try
                        {
                            CreateMessage sendMessage = new CreateMessage("deleteOne",selection);

                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (successor2Port * 2));
                            //socket.setSoTimeout(timeOut);
                            ObjectOutputStream clientOut = new ObjectOutputStream(socket.getOutputStream());
                            clientOut.writeObject(sendMessage);

                            System.out.println("Message being sent to" + successor2Port);
                            //Close all the sockets and input and output stream
                            clientOut.close();
                            socket.close();

                        }catch(Exception e)
                        {
                            e.printStackTrace();
                        }
                    }
                    else if(successor2.equals(myPort))
                    {
                        System.out.println("Deleting one");
                        String delQuery="DELETE FROM "+TABLE_NAME+" WHERE "+firstColumn+"= '"+selection+"';";
                        System.out.println("The delete query is "+delQuery);
                        writingDb.execSQL(delQuery);
                        Log.v("delete done", selection);

                        try
                        {
                            CreateMessage sendMessage = new CreateMessage("deleteOne",selection);

                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (successor1Port * 2));
                            //socket.setSoTimeout(timeOut);
                            ObjectOutputStream clientOut = new ObjectOutputStream(socket.getOutputStream());
                            clientOut.writeObject(sendMessage);

                            System.out.println("Message being sent to" + successor1Port);
                            //Close all the sockets and input and output stream
                            clientOut.close();
                            socket.close();

                        }catch(Exception e)
                        {
                            e.printStackTrace();
                        }
                    }
                }
                else
                {
                    try
                    {
                        CreateMessage sendMessage = new CreateMessage("deleteOne",selection);

                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (successor1Port * 2));
                        //socket.setSoTimeout(timeOut);
                        ObjectOutputStream clientOut = new ObjectOutputStream(socket.getOutputStream());
                        clientOut.writeObject(sendMessage);

                        System.out.println("Message being sent to" + successor1Port);
                        //Close all the sockets and input and output stream
                        clientOut.close();
                        socket.close();

                    }catch(Exception e)
                    {
                        e.printStackTrace();
                    }
                    try
                    {
                        CreateMessage sendMessage = new CreateMessage("deleteOne",selection);

                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (successor2Port * 2));
                        //socket.setSoTimeout(timeOut);
                        ObjectOutputStream clientOut = new ObjectOutputStream(socket.getOutputStream());
                        clientOut.writeObject(sendMessage);

                        System.out.println("Message being sent to" + successor2Port);
                        //Close all the sockets and input and output stream
                        clientOut.close();
                        socket.close();

                    }catch(Exception e)
                    {
                        e.printStackTrace();
                    }
                }

            }

            //Delete from replicas also
        }

        return 0;
    } //END OF DELETE FUNCTION -- MODIFY THIS IF DELETE FAILS

    @Override
    public String getType(Uri uri) {
        //
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values)
    {
        //Check if the the statusActive is true or not
        while(true)
        {
            if(statusActive==true)
            {
                break;
            }
            else
            {
                System.out.println("Sorry still backing up");
                //Means avd backup is going on
                //Thread.sleep(100);
            }
        }

        System.out.println("Inside the INSERT COMMAND");

        String key = values.get("key").toString();
        String value= values.get("value").toString();
        System.out.println("The KEY IN INSERT IS "+key + " AND THE VALUE IS "+value);

        /**
         * this is the normal insert -- Determine whose node is this
         */

        String resultNode = findNode(key);
        System.out.println("FIND NODE IS "+resultNode);

        if(resultNode==myPort)
        {
            //Means I am the coordinator for the key -- Save it and send to Client Task to send the key to successors

            DB_Insert(key,value,myPort);

            HashMap<String,String> temp =  finalRing.get(myPort);
            String successor1 = hashPort.get(temp.get("next1"));
            String successor2 = hashPort.get(temp.get("next2"));

            System.out.println("Insert:: Successor 1 is " + successor1);
            System.out.println("Insert:: Successor 2 is "+successor2);

            String requestMessage = "insert"+delimiter+key+delimiter+value+delimiter+resultNode+delimiter+successor1;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,requestMessage,myPort);

            requestMessage = "insert"+delimiter+key+delimiter+value+delimiter+resultNode+delimiter+successor2;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,requestMessage,myPort);



        /*    //Make the message
            CreateMessage sendMessage = new CreateMessage("insert",key,value,resultNode);

            int sendtoPort;
            try {
                sendtoPort=Integer.parseInt(successor1);
                System.out.println("Message being sent to" + sendtoPort);

                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (sendtoPort * 2));

                ObjectOutputStream clientOut = new ObjectOutputStream(socket.getOutputStream());
                clientOut.writeObject(sendMessage);

                clientOut.close();
                socket.close();


                sendtoPort=Integer.parseInt(successor2);

                System.out.println("Message being sent to" + sendtoPort);

                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (sendtoPort * 2));

                clientOut = new ObjectOutputStream(socket.getOutputStream());
                clientOut.writeObject(sendMessage);

                clientOut.close();
                socket.close();

            }catch (Exception e)
            {
                System.out.println("Nothing to be done");
            }
*/



        }//End of checking whether key belongs to my port
        else
        {
            //means the key does not belong to me -- Send to all three in one go
            // I am not the co-ordinator but I might be first or second successor -- So directly save in me


            //Call a function to send to the coordinator and wait for ack

            String requestMessage = "insert"+delimiter+key+delimiter+value+delimiter+resultNode;
            System.out.println("Value to send to handle insert is "+requestMessage);
            HandleInsert(requestMessage);



        }


        return uri;
    }

    public void HandleInsert (String message)
    {


        System.out.println("Value received at handle insert is "+message);
        String [] messageArray = message.split(delimiter);

        HashMap<String,String> temp =  finalRing.get(messageArray[3]);
        String successor1 = hashPort.get(temp.get("next1"));
        String successor2 = hashPort.get(temp.get("next2"));

        System.out.println("Successor 1 is "+successor1);
        System.out.println("Successor 2 is "+successor2);


        String requestMessage="insert"+delimiter+messageArray[1]+delimiter+messageArray[2]+delimiter+messageArray[3]+delimiter+messageArray[3];
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,requestMessage,myPort);

        if(successor1.equals(myPort) || successor2.equals(myPort))
        {
            if(successor1.equals(myPort))
            {
                //Means I save in myself and send only to second successor
                DB_Insert(messageArray[1],messageArray[2],messageArray[3]);

                requestMessage="insert"+delimiter+messageArray[1]+delimiter+messageArray[2]+delimiter+messageArray[3]+delimiter+successor2;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,requestMessage,myPort);

            }
            else
            {
                //Means I save in myself and send only to first successor
                DB_Insert(messageArray[1],messageArray[2],messageArray[3]);

                requestMessage="insert"+delimiter+messageArray[1]+delimiter+messageArray[2]+delimiter+messageArray[3]+delimiter+successor1;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,requestMessage,myPort);

            }
        }else
        {
            requestMessage="insert"+delimiter+messageArray[1]+delimiter+messageArray[2]+delimiter+messageArray[3]+delimiter+successor1;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,requestMessage,myPort);

            requestMessage="insert"+delimiter+messageArray[1]+delimiter+messageArray[2]+delimiter+messageArray[3]+delimiter+successor2;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,requestMessage,myPort);

        }













/*
        //Make the message
        CreateMessage sendMessage = new CreateMessage(messageArray[0], messageArray[1],messageArray[2], messageArray[3]);

        int sendtoPort;
        try
        {
            sendtoPort=Integer.parseInt(messageArray[3]);
            System.out.println("Message key"+ messageArray[2]+ " is being sent to" + sendtoPort);

            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (sendtoPort * 2));

            ObjectOutputStream clientOut = new ObjectOutputStream(socket.getOutputStream());
            clientOut.writeObject(sendMessage);

            clientOut.close();
            socket.close();


            if(successor1.equals(myPort) || successor2.equals(myPort))
            {
                //Either I am successor 1 or successor 2
                if(successor1.equals(myPort))
                {
                    //Means I save in myself and send only to second successor
                    DB_Insert(messageArray[1],messageArray[2],messageArray[3]);

                    sendtoPort=Integer.parseInt(successor2);
                    System.out.println("Message key"+ messageArray[2]+ " is being sent to" + sendtoPort);

                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (sendtoPort * 2));
                    socket.setSoTimeout(timeOut);
                    clientOut = new ObjectOutputStream(socket.getOutputStream());
                    clientOut.writeObject(sendMessage);

                    clientOut.close();
                    socket.close();



                }
                else if(successor2.equals(myPort))
                {
                    //Means I save in myself and send only to first successor
                    DB_Insert(messageArray[1],messageArray[2],messageArray[3]);

                    sendtoPort=Integer.parseInt(successor1);
                    System.out.println("Message key"+ messageArray[2]+ " is being sent to" + sendtoPort);

                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (sendtoPort * 2));
                    socket.setSoTimeout(timeOut);
                    clientOut = new ObjectOutputStream(socket.getOutputStream());
                    clientOut.writeObject(sendMessage);

                    clientOut.close();
                    socket.close();

                }
            }
            else
            {
                sendtoPort=Integer.parseInt(successor1);
                System.out.println("Message being sent to" + sendtoPort);

                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (sendtoPort * 2));
                socket.setSoTimeout(timeOut);
                clientOut = new ObjectOutputStream(socket.getOutputStream());
                clientOut.writeObject(sendMessage);

                clientOut.close();
                socket.close();

                sendtoPort=Integer.parseInt(successor2);
                System.out.println("Message being sent to" + sendtoPort);

                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (sendtoPort * 2));
                socket.setSoTimeout(timeOut);
                clientOut = new ObjectOutputStream(socket.getOutputStream());
                clientOut.writeObject(sendMessage);

                clientOut.close();
                socket.close();

            }



        }catch (Exception e)
        {
            System.out.println("Do Nothing");
        }
*/

    }

    void sendSuccessorInsert(String requestMessage)
    {
        String [] messageArray = requestMessage.split(delimiter);

        HashMap<String,String> temp =  finalRing.get(messageArray[3]);
        String successor1 = hashPort.get(temp.get("next1"));
        String successor2 = hashPort.get(temp.get("next2"));

        System.out.println("Successor 1 is "+successor1);
        System.out.println("Successor 2 is "+successor2);

        //SEND REPLICATE TO BOTH THE SUCCESSORS THROUGH A SEPARATE CLIENT TASK
        /**
         * First Check if you are successor 1 then save it in DB AND FAILURE MAP or else send it to client task with type replicate to send to second successor
         * Next Check if you are successor 2 then save it or else send it to client task with type replicate
         */

        if(successor1.equals(myPort) || successor2.equals(myPort))
        {
            //Save it in my own DB
            DB_Insert(messageArray[1],messageArray[2],messageArray[3]);
            System.out.println("Insert in Handle Insert with key" + messageArray[1]+ " with value "+ messageArray[2]);

            //Save it in failure map
            //KEY ->  AVD(FOR)+delimiter+UniqueFAILUREID
            //VALUE -> INSERT+delimiter+key+delimiter+value
            //Send key as AVD supposed for and value as operation and parameters

            saveFailureMap(messageArray[3],"insert"+delimiter+messageArray[1]+delimiter+messageArray[2]);

            if(successor1.equals(myPort))
            {
                //Send to second successor via Client
                requestMessage="replicate"+delimiter+messageArray[1]+delimiter+messageArray[2]+delimiter+messageArray[3]+delimiter+successor2;
                System.out.println("Sending message to replicate is "+requestMessage);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, requestMessage, myPort);
            }
            else
            {
                //Send to first successor via Client
                requestMessage="replicate"+delimiter+messageArray[1]+delimiter+messageArray[2]+delimiter+messageArray[3]+delimiter+successor1;
                System.out.println("Sending message to replicate is "+requestMessage);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, requestMessage, myPort);
//                System.out.println("I am the second Successor and I am not supposed to replicate it further");
            }

        }
        else             //I am not Successor 1 or Successor 2
        {
            //Send to both Successor1 and Successor2 via Client

            requestMessage="replicate"+delimiter+messageArray[1]+delimiter+messageArray[2]+delimiter+messageArray[3]+delimiter+successor1;
            System.out.println("Sending message to replicate is "+requestMessage);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, requestMessage, myPort);

            requestMessage="replicate"+delimiter+messageArray[1]+delimiter+messageArray[2]+delimiter+messageArray[3]+delimiter+successor2;
            System.out.println("Sending message to replicate is "+requestMessage);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, requestMessage, myPort);

        }
    }

    //When no synchronization is allowed -- TO BE USED IN CASE OF RECOVERY
    public void DB_Insert(String key,String value,String avd)
    {
        try
        {
            Cursor reader = null;

            String getQuery = "SELECT * FROM "+ TABLE_NAME +" WHERE " + firstColumn + " = '" + key +"';";
            System.out.println("USER:: The get query is "+getQuery);
            reader = readingDb.rawQuery(getQuery,null);

            if(reader.moveToFirst())
            {
                //Entry exists
                //Get version value

                int oldVersion= reader.getInt(reader.getColumnIndex("version"));
                String savedValue = reader.getString(reader.getColumnIndex("value"));

                //ONLY save if the value is different
                if(!(value.equals(savedValue)))
                {
                    //Meaning value is not the same
                    int newVersion = oldVersion+1;
                    String updateQuery ="UPDATE "+TABLE_NAME+" SET value ='"+value+"' , version = "+newVersion+" , avd = '"+avd+"' WHERE key = '"+key+"';";
                    //String updateQuery ="UPDATE "+TABLE_NAME+" SET value ='"+value+"' , version = "+newVersion+" WHERE key = '"+key+"';";
                    System.out.println("Update Query:"+updateQuery);
                    writingDb.execSQL(updateQuery);
                }
            }
            else
            {
                //Entry Does not exist
                int ver=1;
                String insertQuery ="INSERT INTO "+TABLE_NAME+" VALUES('"+key+"', '"+value+"', '"+avd+"', "+ver+" );";
                System.out.println("Insert query is"+insertQuery);
                writingDb.execSQL(insertQuery);
            }

            reader.close();

        }catch(Exception e)
        {
            System.out.println("Exception occurred while saving own Key to DB ");
            e.printStackTrace();
        }

    }
    public void Fast_DB_Insert(String key,String value,String avd)
    {
        try
        {
            Cursor reader = null;

            String getQuery = "SELECT * FROM "+ TABLE_NAME +" WHERE " + firstColumn + " = '" + key +"';";
            System.out.println("USER:: The get query is "+getQuery);
            reader = readingDb.rawQuery(getQuery,null);

            if(reader.moveToFirst())
            {
                //Entry exists
                //Get version value

                int oldVersion= reader.getInt(reader.getColumnIndex("version"));
                String savedValue = reader.getString(reader.getColumnIndex("value"));

                //ONLY save if the value is different
                if(!(value.equals(savedValue)))
                {
                    //Meaning value is not the same
                    int newVersion = oldVersion+1;
                    String updateQuery ="UPDATE "+TABLE_NAME+" SET value ='"+value+"' , version = "+newVersion+" , avd = '"+avd+"' WHERE key = '"+key+"';";
                    System.out.println("Update Query:"+updateQuery);
                    writingDb.execSQL(updateQuery);
                }
            }
            else
            {
                //Entry Does not exist
                int ver=1;
                String insertQuery ="INSERT INTO "+TABLE_NAME+" VALUES('"+key+"', '"+value+"', '"+avd+"', "+ver+" );";
                System.out.println("Insert query is"+insertQuery);
                writingDb.execSQL(insertQuery);
            }

            reader.close();

        }catch(Exception e)
        {
            System.out.println("Exception occurred while saving own Key to DB ");
            e.printStackTrace();
        }

    }


    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                                      String[] selectionArgs, String sortOrder)
    {
        System.out.println("USER::This is inside query function");
        Cursor readerRet=null;
        System.out.println("USER::QUERY FUNCTION:: selection is "+selection);

        //Check if the the statusActive is true or not
        while(true)
        {
            if(statusActive==true)
            {
                System.out.println("Status is active now");
                break;
            }
            else
            {
                System.out.println("Sorry still backing up");
                //Means avd backup is going on
                //Thread.sleep(100);
            }
        }

        if(selection.equals("\"*\""))
        {
            // >> * >> Retrieve values from entire DHT

            try
            {
                Cursor reader;
                String getQuery = "SELECT * FROM " + TABLE_NAME + " WHERE avd " + " = '" + myPort + "';";
                System.out.println("USER:: The get query is " + getQuery);
                reader = readingDb.rawQuery(getQuery, null);


                int keyIndex;
                int valueIndex;
                String returnKey;

                String returnValue;
                for (boolean item = reader.moveToFirst(); item; item = reader.moveToNext()) {

                    returnValue = reader.getString(reader.getColumnIndex("value"));
                    returnKey = reader.getString(reader.getColumnIndex("key"));
                    System.out.println("While retrieving all values by * --> key is "+returnKey+ " and value is "+returnValue);
                    //Save in global hashMap
                    AllReply.put(returnKey, returnValue);
                }

                replyCount=1;

                for (String aPortArray : portArray) {
                    if (aPortArray.equals(myPort))
                        continue;

                    String sendPort = aPortArray;
                    String requestMessage = "Retrieveall" + delimiter + sendPort;
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, requestMessage, myPort);

                }

                System.out.println("Entering the waiting channel");

                while(true)
                {

                    if(replyCount==5)
                        break;
                    else
                        Thread.sleep(100);
                }

                System.out.println("out of wait channel");

                String[] columnNames = new String[]{firstColumn, secondColumn};
                MatrixCursor makingCursor = new MatrixCursor(columnNames);
                String allKey=null;
                String allValue=null;
                System.out.println("Using KeySet");
                for(String key: AllReply.keySet())
                {
                    System.out.println(key + " :: " + AllReply.get(key));
                    allKey = key;
                    allValue = AllReply.get(key);
                    makingCursor.addRow(new Object[]{allKey, allValue});

                }
                //Clear the single results hashMap

                Log.v("All Query back", "Success");
                replyCount = 0;
                AllReply.clear();

                return makingCursor;

            }catch (Exception e)
            {
                e.printStackTrace();
            }


        }
        else if(selection.equals("\"@\""))
        {
            // >> @ >> Retrieve values from own AVD
            String getQuery = "SELECT key, value FROM "+ TABLE_NAME +";";
            System.out.println("USER:: The get query is "+getQuery);

            Cursor reader=null;
            try{
                if(!(readingDb.isOpen()))
                    System.out.println("Readingdb is closed");

                reader = readingDb.rawQuery(getQuery,selectionArgs);
                readerRet=reader;
                return reader;

            }catch(SQLiteException e){
                e.printStackTrace();
            }catch(Exception e){
                e.printStackTrace();
            }

            Log.v("query", selection);


        }
        else //Querying for a single key
        {
            //Querying for the particular key
            System.out.println("Querying a particular key:: "+selection);
            //Check which node it lies in
            String resultNode = findNode(selection);
            System.out.println("FIND NODE IS (in QUERY)"+resultNode);

            if(resultNode.equals(myPort)) //Checking if I am the coordinator for the key
            {

                //Meaning I am the co-ordinator for the query - Maintaining that co-ordinator always has the latest value
                //Retrieve the value from own DB
                String savedValue=null;
                try
                {
                    Cursor reader = null;
                    String getQuery = "SELECT * FROM " + TABLE_NAME + " WHERE " + firstColumn + " = '" + selection + "';";
                    System.out.println("USER:: The get query is " + getQuery);
                    reader = readingDb.rawQuery(getQuery, null);

                    if (reader.moveToFirst())
                    {
                        //Entry exists

                        int version = reader.getInt(reader.getColumnIndex("version"));
                        savedValue = reader.getString(reader.getColumnIndex("value"));
                        System.out.println("The value retrieved for key: "+selection+" is value: "+savedValue+" version: "+version);

                        String[] columnNames = new String[]{firstColumn, secondColumn};
                        MatrixCursor makingCursor = new MatrixCursor(columnNames);

                        makingCursor.addRow(new Object[]{selection, savedValue});

                        System.out.println("Returned the query when it was with me for key "+selection);
                        return makingCursor;

                    }else
                    {
                        //No such entry exists
                        //Add nothing with version as 0
                        System.out.println("No such entry exists");
                    }


                }catch (SQLiteException e)
                {
                    e.printStackTrace();
                }catch (Exception e)
                {
                    e.printStackTrace();
                }
            }
            else  //Meaning I am not the coordinator for the key
            {
                //get the value for the key from the coordinator

                System.out.println("I am not the coordinator for this key:: "+selection+ "which belongs to : "+resultNode);

                int coordinatorPort = Integer.parseInt(resultNode);
                //Call Client Task to Handle Inserts to successors
                HashMap<String,String> temp =  finalRing.get(resultNode);

                String successor1 = hashPort.get(temp.get("next1"));
                String successor2 = hashPort.get(temp.get("next2"));

                System.out.println("Successor 1 is "+successor1);
                System.out.println("Successor 2 is "+successor2);

                if (successor1.equals(myPort) || successor2.equals(myPort))
                {
                    String savedValue;
                    //Retrieve and return from me only
                    Cursor reader = null;
                    String getQuery = "SELECT * FROM " + TABLE_NAME + " WHERE " + firstColumn + " = '" + selection + "';";
                    System.out.println("USER:: The get query is " + getQuery);
                    reader = readingDb.rawQuery(getQuery, null);

                    if (reader.moveToFirst()) {
                        //Entry exists

                        int version = reader.getInt(reader.getColumnIndex("version"));
                        savedValue = reader.getString(reader.getColumnIndex("value"));
                        System.out.println("The value retrieved for key: " + selection + " is value: " + savedValue + " version: " + version);

                        String[] columnNames = new String[]{firstColumn, secondColumn};
                        MatrixCursor makingCursor = new MatrixCursor(columnNames);

                        makingCursor.addRow(new Object[]{selection, savedValue});

                        System.out.println("Returned the key "+selection + " with value "+savedValue);
                        System.out.println("Returned the query when it was not with me for key "+selection);

                        return makingCursor;

                    }

                }

                try
                {
                    //Make the message
                    CreateMessage sendMessage = new CreateMessage("query", selection,resultNode);

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (coordinatorPort * 2));
                    socket.setSoTimeout(timeOut);
                    ObjectOutputStream clientOut = new ObjectOutputStream(socket.getOutputStream());
                    clientOut.writeObject(sendMessage);

                    System.out.println("Message being sent to" + coordinatorPort);

                    System.out.println("Waiting for the message");

                    ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                    CreateMessage recvMessage = (CreateMessage) in.readObject();

                    System.out.println("Message reply received ::with message type" + recvMessage.messageType);
                    System.out.println("Key received is "+recvMessage.key + " ANd value is "+recvMessage.value);

                    String[] columnNames = new String[]{firstColumn, secondColumn};
                    MatrixCursor makingCursor = new MatrixCursor(columnNames);

                    makingCursor.addRow(new Object[]{selection, recvMessage.value});

                    //Close all the sockets and input and output stream
                    clientOut.close();
                    in.close();
                    socket.close();
                    System.out.println("Returned the query when it was not with me for key "+selection);

                    return makingCursor;


                }catch(Exception e) { // Failed querying from Coordinator

                    System.out.println("Socket Timeout happened while waiting for ack while Querying");
                    System.out.println("Now Query Successor1");
                    try
                    {

                        String result = querySuccessor(selection,successor2,resultNode);

                        String[] columnNames = new String[]{firstColumn, secondColumn};
                        MatrixCursor makingCursor = new MatrixCursor(columnNames);

                        makingCursor.addRow(new Object[]{selection, result});

                        System.out.println("Returned the key "+selection + " with value "+result);
                        System.out.println("Returned the query when it was not with me for key "+selection);

                        return makingCursor;


                    }catch (Exception esa)
                    {
                        System.out.println("Meaning Query from first successor did not work -- Call function again to the second one");
                        try
                        {
                            String result = querySuccessor(selection,successor1,resultNode);

                            String[] columnNames = new String[]{firstColumn, secondColumn};
                            MatrixCursor makingCursor = new MatrixCursor(columnNames);

                            makingCursor.addRow(new Object[]{selection, result});

                            System.out.println("Returned the key "+selection + " with value "+result);

                            System.out.println("Returned the query when it was not with me for key "+selection);

                            return makingCursor;


                        }catch (Exception es)
                        {
                            es.printStackTrace();
                            System.out.println("Meaning Query from second successor did not work ");
                        }

                    }
                }

            }
        }

        return readerRet;
    }

    public String querySuccessor(String selection,String successorPort,String resultNode) throws Exception,SocketTimeoutException, SocketException
    {
        String returnkeyValue=null;

        HashMap<String,String> temp =  finalRing.get(resultNode);

        String successor11 = hashPort.get(temp.get("next1"));
        String successor22 = hashPort.get(temp.get("next2"));

        System.out.println("Successor 1 is "+successor11);
        System.out.println("Successor 2 is "+successor22);
        String savedValue=null;
        if (successor11.equals(myPort) || successor22.equals(myPort))
        {
            //Retrieve and return from me only
            Cursor reader = null;
            String getQuery = "SELECT * FROM " + TABLE_NAME + " WHERE " + firstColumn + " = '" + selection + "';";
            System.out.println("USER:: The get query is " + getQuery);
            reader = readingDb.rawQuery(getQuery, null);

            if (reader.moveToFirst()) {
                //Entry exists

                int version = reader.getInt(reader.getColumnIndex("version"));
                savedValue = reader.getString(reader.getColumnIndex("value"));
                System.out.println("The value retrieved for key: " + selection + " is value: " + savedValue + " version: " + version);

                returnkeyValue=savedValue;
                return returnkeyValue;

            }

        }

        System.out.println("The key to query is "+selection+" from "+successorPort);
        if(successorPort.equals(myPort))
        {
            //Query from own DB and return value as value
            //Retrieve the value from own DB


            Cursor reader = null;
            String getQuery = "SELECT * FROM " + TABLE_NAME + " WHERE " + firstColumn + " = '" + selection + "';";
            System.out.println("USER:: The get query is " + getQuery);
            reader = readingDb.rawQuery(getQuery, null);

            if (reader.moveToFirst()) {
                //Entry exists

                int version = reader.getInt(reader.getColumnIndex("version"));
                savedValue = reader.getString(reader.getColumnIndex("value"));
                System.out.println("The value retrieved for key: " + selection + " is value: " + savedValue + " version: " + version);

                returnkeyValue=savedValue;

            } else {
                //No such entry exists
                //Add nothing with version as 0
                System.out.println("No such entry exists");
            }

        }
        else
        {
            int successorPor = Integer.parseInt(successorPort);

            //Send query message to the successor and wait for reply and then send that reply back to the grader
            //Make the message
            CreateMessage sendMessage = new CreateMessage("query", selection,resultNode);

            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (successorPor * 2));
            socket.setSoTimeout(timeOut);
            ObjectOutputStream clientOut = new ObjectOutputStream(socket.getOutputStream());
            clientOut.writeObject(sendMessage);

            System.out.println("Message being sent to" + successorPor);

            System.out.println("Waiting for the message");

            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            CreateMessage recvMessage = (CreateMessage) in.readObject();

            System.out.println("Message reply received ::with message type" + recvMessage.messageType);
            System.out.println("Key received is "+recvMessage.key + " ANd value is "+recvMessage.value);

            //Close all the sockets and input and output stream
            clientOut.close();
            in.close();
            socket.close();

            returnkeyValue=recvMessage.value;
        }
        return returnkeyValue;
    }




    public synchronized int getFailuniqueNum()
    {
        int returnValue= failuniqueNum;

        failuniqueNum++;

        return returnValue;
    }

    //KEY ->  AVD(FOR)+delimiter+UniqueFAILUREID
    //VALUE -> insert+delimiter+key+delimiter+value
    //Send key as AVD supposed for and value as operation and parameters
    public void saveFailureMap(String key,String value)
    {
        //Get the failureUniqueNum and append with key
        int uniqueNum = getFailuniqueNum();
        String saveKey = Integer.toString(uniqueNum)+delimiter+key;


        failureMap.put(saveKey,value);
    }

    public void saveinAllMap(HashMap<String,String> newMap)
    {
        AllReply.putAll(newMap);
        replyCount++;
    }

    /*
        Now make the recovery data based on whether your port is succ1 or succ2 or prev1 or prev2 for the requesting recovery port
     */
    public void saveinRecoveryMap(String port,HashMap<String,String> temp)
    {
        HashMap<String,String> newMap = new HashMap<String,String> ();

        for(String key: temp.keySet()){
            System.out.println(key  +" :: "+ temp.get(key));
            newMap.put(key,temp.get(key));
        }

        recoverData.put(port,newMap);

    }


    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {

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

    //Function to find the right co-ordinator
    public String findNode(String value)
    {
        String node=null;
        try {

            String valueHash= genHash(value);

            int prev=0;
          //  System.out.println("Original Key list is ");
            for(int counter=0;counter<keyList.size();counter++)
            {
                //Iterating the sorted list
                // System.out.println("They port is is "+hashPort.get(keyList.get(counter))+ " and hash is "+keyList.get(counter));

                if(counter==0)
                {
                    prev= (keyList.size() - 1);
                }
                else
                {
                    prev= counter-1;
                }

                String selfHash = keyList.get(counter);
                String prev1 = keyList.get(prev);

                int c1 = valueHash.compareTo(selfHash);
                int c2 = valueHash.compareTo(prev1);

                if (c1 < 0 && c2 > 0) {
                    //Means this is the node where it should be saved
                    node = hashPort.get(selfHash);
                    return node;
                }

            }
            if(node==null)
            {
                // System.out.println("Value received is null");

                node=hashPort.get(keyList.get(0));
            }
        }catch (NoSuchAlgorithmException e)
        {
            e.printStackTrace();
        }
        return node;
    }





    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets)
        {
            ServerSocket serverSocket = sockets[0];
            try {
                while (true)
                {
                    Socket clientSocket = serverSocket.accept();


                    //Check if the the statusActive is true or not
                    while(true)
                    {
                        if(statusActive==true)
                        {
                            break;
                        }
                        else
                        {
                            System.out.println("Sorry still backing up");
                            //Means avd backup is going on
                            //Thread.sleep(100);
                        }
                    }

                    System.out.println("USER::: We are in Server Task");

                    ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream());
                    CreateMessage recvMessageObj = (CreateMessage) in.readObject();

                    if (recvMessageObj != null)
                    {
                        //Convert received inline message to type of MessageClass
                        System.out.println("USER:: SERVER TASK: Message received is " + recvMessageObj.messageType);


                        if (recvMessageObj.messageType.equals("insert"))
                        {
                            //Means that u get the message to replicate from the original coordinator


                            String  key=recvMessageObj.key;
                            String  value=recvMessageObj.value;
                            String  originalPort=recvMessageObj.originalPort;
                            System.out.println("Insert in server task with key" +key+ " with value "+value);
                            //Call insert to directly save it
                            DB_Insert(key,value,originalPort);

              /*              //Send ack back to the client message
                            CreateMessage sendMessage = new CreateMessage("insertSaved");
                            ObjectOutputStream clientOut = new ObjectOutputStream(clientSocket.getOutputStream());
                            clientOut.writeObject(sendMessage);

                            clientOut.close();
*/
                        }
                        else if (recvMessageObj.messageType.equals("insertyour"))
                        {
                            //Co-ordinator receives the message from another AVD to insert and send to successors

                            String key=recvMessageObj.key;
                            String value = recvMessageObj.value;
                            String originalPort = recvMessageObj.originalPort;

                            DB_Insert(key,value,originalPort);
                            System.out.println("Insert in server task with key" +key+ " with value "+value);

                            //Send back ACK to the receiver of the key
                            CreateMessage sendMessage = new CreateMessage("insertSaved");
                            ObjectOutputStream clientOut = new ObjectOutputStream(clientSocket.getOutputStream());
                            clientOut.writeObject(sendMessage);


                            //Call Client Task to Handle Inserts to successors
                            HashMap<String,String> temp =  finalRing.get(myPort);
                            String successor1 = hashPort.get(temp.get("next1"));
                            String successor2 = hashPort.get(temp.get("next2"));

                            System.out.println("Successor 1 is "+successor1);
                            System.out.println("Successor 2 is "+successor2);

                            String requestMessage = "insert"+delimiter+key+delimiter+value+delimiter+originalPort+delimiter+successor1;
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,requestMessage,myPort);

                            requestMessage = "insert"+delimiter+key+delimiter+value+delimiter+originalPort+delimiter+successor2;
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,requestMessage,myPort);

                        }
                        else if (recvMessageObj.messageType.equals("replicate"))
                        {
                            //Message came in order to replicate after failure
                            String key = recvMessageObj.key;
                            String value = recvMessageObj.value;
                            String originalPort = recvMessageObj.originalPort;

                            DB_Insert(key,value,originalPort);
                            System.out.println("Insert in server task with key" +key+ " with value "+value);


                            //KEY ->  AVD(FOR)+delimiter+UniqueFAILUREID
                            //VALUE -> INSERT+delimiter+key+delimiter+value
                            //Send key as AVD supposed for and value as operation and parameters

                            String putKey = originalPort;
                            String putValue = "insert"+delimiter+key+delimiter+value;
                            saveFailureMap(putKey,putValue);

                        }
                        else if (recvMessageObj.messageType.equals("query"))
                        {
                            String selection= recvMessageObj.key;
                            System.out.println("The selection received is "+selection);

                            int version;
                            String savedValue;

                            //Query the DB and retrieve the value
                            try
                            {
                                Cursor reader = null;
                                String getQuery = "SELECT * FROM " + TABLE_NAME + " WHERE " + firstColumn + " = '" + selection + "';";
                                System.out.println("USER:: The get query is " + getQuery);
                                reader = readingDb.rawQuery(getQuery, null);

                                if(reader.moveToFirst())
                                {
                                    //Entry exists
                                    version = reader.getInt(reader.getColumnIndex("version"));
                                    savedValue = reader.getString(reader.getColumnIndex("value"));
                                    System.out.println("The value retrieved for key: "+selection+" is value: "+savedValue+" version: "+version);

                                    //Send ack back to the client message
                                    CreateMessage sendMessage = new CreateMessage("queryReply",recvMessageObj.key,savedValue);
                                    ObjectOutputStream clientOut = new ObjectOutputStream(clientSocket.getOutputStream());
                                    clientOut.writeObject(sendMessage);

                                    clientOut.close();

                                }


                            }catch (SQLiteException e)
                            {
                                e.printStackTrace();
                            }catch (Exception e)
                            {
                                e.printStackTrace();
                            }
                        }
                        else if (recvMessageObj.messageType.equals("deleteAll"))
                        {
                            try
                            {
                                String deleteQuery = "SELECT * FROM " + TABLE_NAME +";";
                                System.out.println("USER:: The delete query is " + deleteQuery);
                                writingDb.delete(TABLE_NAME,null,null);

                            }catch(Exception e)
                            {
                                e.printStackTrace();
                            }

                        }
                        else if (recvMessageObj.messageType.equals("deleteOne"))
                        {
                            String selection = recvMessageObj.key;
                            System.out.println("Deleting one");
                            String delQuery="DELETE FROM "+TABLE_NAME+" WHERE "+firstColumn+"= '"+selection+"';";
                            System.out.println("The delete query is "+delQuery);
                            writingDb.execSQL(delQuery);
                            Log.v("delete done", selection);
                        }
                        else if (recvMessageObj.messageType.equals("resurrection"))
                        {
                            //Received at server task -- requesting for data for resurrection

                            //First Determine your relationship to the requester of resurrection

                            String selection = recvMessageObj.key;
                            HashMap<String,String> temp =  finalRing.get(selection);
                            String successor1 = hashPort.get(temp.get("next1"));
                            String successor2 = hashPort.get(temp.get("next2"));
                            String prev1 = hashPort.get(temp.get("prev1"));
                            String prev2 = hashPort.get(temp.get("prev2"));

                            System.out.println("RECOVERY ::Successor 1 is "+successor1);
                            System.out.println("RECOVERY ::Successor 2 is "+successor2);
                            System.out.println("RECOVERY ::Prev 1 is "+prev1);
                            System.out.println("RECOVERY ::Prev 2 is "+prev2);

                            if(myPort.equals(successor1) || myPort.equals(successor2))
                            {
                                //I AM the successor - send data for AVD requested for in the key
                                Cursor reader;
                                String getQuery = "SELECT * FROM " + TABLE_NAME + " WHERE avd " + " = '" + selection + "';";
                                System.out.println("USER:: The get query is " + getQuery);
                                reader = readingDb.rawQuery(getQuery, null);

                                int keyIndex;
                                int valueIndex;
                                String returnKey;
                                HashMap<String,String> tempMap = new HashMap<String,String>();

                                String returnValue;
                                for (boolean item = reader.moveToFirst(); item; item = reader.moveToNext()) {

                                    returnValue = reader.getString(reader.getColumnIndex("value"));
                                    returnKey = reader.getString(reader.getColumnIndex("key"));

                                    //Save in global hashMap
                                    tempMap.put(returnKey, returnValue);
                                }

                                //Send ack back to the client message
                                CreateMessage sendMessage = new CreateMessage("RecoverNow",tempMap);
                                ObjectOutputStream clientOut = new ObjectOutputStream(clientSocket.getOutputStream());
                                clientOut.writeObject(sendMessage);

                                tempMap.clear();
                                clientOut.close();

                            }
                            else if(myPort.equals(prev1) || myPort.equals(prev2))
                            {
                                //Means my data has to be replicated on the the requester
                                Cursor reader;
                                String getQuery = "SELECT * FROM " + TABLE_NAME + " WHERE avd " + " = '" + myPort + "';";
                                System.out.println("USER:: The get query is " + getQuery);
                                reader = readingDb.rawQuery(getQuery, null);

                                int keyIndex;
                                int valueIndex;
                                String returnKey;
                                HashMap<String,String> tempMap = new HashMap<String,String>();

                                String returnValue;
                                for (boolean item = reader.moveToFirst(); item; item = reader.moveToNext()) {

                                    returnValue = reader.getString(reader.getColumnIndex("value"));
                                    returnKey = reader.getString(reader.getColumnIndex("key"));

                                    //Save in global hashMap
                                    tempMap.put(returnKey, returnValue);
                                }

                                //Send ack back to the client message
                                CreateMessage sendMessage = new CreateMessage("RecoverNow",tempMap);
                                ObjectOutputStream clientOut = new ObjectOutputStream(clientSocket.getOutputStream());
                                clientOut.writeObject(sendMessage);

                                tempMap.clear();
                                clientOut.close();

                            }

                            /*System.out.println("Printing Failure MAP DATA");
                            System.out.println("Using KeySet");
                            for(String key: failureMap.keySet()){
                                System.out.println(key  +" :: "+ failureMap.get(key));

                            }*/
                            System.out.println("Sent recovery Data");

                        }//End of Resurrection
                        else if (recvMessageObj.messageType.equals("Retrieveall"))
                        {
                            try
                            {
                                Cursor reader;
                                String getQuery = "SELECT * FROM " + TABLE_NAME + " WHERE avd " + " = '" + myPort + "';";
                                System.out.println("USER:: The get query is " + getQuery);
                                reader = readingDb.rawQuery(getQuery, null);


                                int keyIndex;
                                int valueIndex;
                                String returnKey;
                                HashMap<String,String> tempMap = new HashMap<String,String>();

                                String returnValue;
                                for (boolean item = reader.moveToFirst(); item; item = reader.moveToNext()) {

                                    returnValue = reader.getString(reader.getColumnIndex("value"));
                                    returnKey = reader.getString(reader.getColumnIndex("key"));

                                    //Save in global hashMap
                                    tempMap.put(returnKey, returnValue);
                                }

                                //Send ack back to the client message
                                CreateMessage sendMessage = new CreateMessage("retrieveallreply",tempMap);
                                ObjectOutputStream clientOut = new ObjectOutputStream(clientSocket.getOutputStream());
                                clientOut.writeObject(sendMessage);

                                tempMap.clear();
                                clientOut.close();

                            }catch(Exception e)
                            {
                                e.printStackTrace();
                            }

                        }
                        else if (recvMessageObj.messageType.equals("getallforother"))
                        {

                            try
                            {
                                Cursor reader;
                                String getQuery = "SELECT * FROM " + TABLE_NAME + " WHERE avd " + " = '" + recvMessageObj.key + "';";
                                System.out.println("USER:: The get query is " + getQuery);
                                reader = readingDb.rawQuery(getQuery, null);


                                int keyIndex;
                                int valueIndex;
                                String returnKey;
                                HashMap<String,String> tempMap = new HashMap<String,String>();

                                String returnValue;
                                for (boolean item = reader.moveToFirst(); item; item = reader.moveToNext()) {

                                    returnValue = reader.getString(reader.getColumnIndex("value"));
                                    returnKey = reader.getString(reader.getColumnIndex("key"));

                                    //Save in global hashMap
                                    tempMap.put(returnKey, returnValue);
                                }

                                //Send ack back to the client message
                                CreateMessage sendMessage = new CreateMessage("reply",tempMap);
                                ObjectOutputStream clientOut = new ObjectOutputStream(clientSocket.getOutputStream());
                                clientOut.writeObject(sendMessage);

                                tempMap.clear();
                                clientOut.close();

                            }catch(Exception e)
                            {
                                e.printStackTrace();
                            }

                        }


                    }//END of if checking whether recvMessageObj is not null

                    in.close();
                }

            } catch (IOException e) {
                e.printStackTrace();
                Log.e(TAG, "Error creating socket on accepting/while listening on a port");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                Log.e(TAG, "Class conversion not found");
            } catch (Exception e) {
                e.printStackTrace();
                Log.e(TAG, e.toString());

            }
            return null;
        } // End of Do in background task

    }


    private class ClientTask extends AsyncTask<String, Void, Void>
    {

        @Override
        protected Void doInBackground(String... msgs) {

            System.out.println("USER::: We are in ClientTask");
            System.out.println("Message to send is "+msgs[0]);
            String [] messageArray = msgs[0].split(delimiter);


            if(messageArray[0].equals("insert"))
            {

                int successorPort = Integer.parseInt(messageArray[4]);
                try
                {
                    //Make the message
                    CreateMessage sendMessage = new CreateMessage(messageArray[0], messageArray[1],
                            messageArray[2], messageArray[3]);
                    System.out.println("Message being sent to" + successorPort);
                    System.out.println("Message is" + msgs[0]);

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (successorPort * 2));
                    socket.setSoTimeout(timeOut);
                    ObjectOutputStream clientOut = new ObjectOutputStream(socket.getOutputStream());
                    clientOut.writeObject(sendMessage);

                    System.out.println("Message being sent to" + successorPort);

                 /*   System.out.println("Waiting for the message");

                    ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                    CreateMessage recvMessage = (CreateMessage) in.readObject();

                    System.out.println("Message reply received ::with message type" + recvMessage.messageType);
                 */   //ACk Received

                    //Close all the sockets and input and output stream
                    clientOut.close();
                    socket.close();

                }catch(Exception e)
                {
                    System.out.println("Socket Timeout happened while waiting for ack from successor");
                    //Means no ACK received -- Get an unique number and save to hashmap

                    //KEY ->  AVD(FOR)+delimiter+UniqueFAILUREID
                    //VALUE -> insert+delimiter+key+delimiter+value
                    //Send key as AVD supposed for and value as operation and parameters


                 //   saveFailureMap(messageArray[4],"insert"+delimiter+messageArray[1]+delimiter+messageArray[2]);

                    e.printStackTrace();
                }

            } //End of type-- insert
            else if(messageArray[0].equals("replicate"))
            {

                int successorPort = Integer.parseInt(messageArray[4]);
                try
                {
                    //Make the message
                    CreateMessage sendMessage = new CreateMessage(messageArray[0], messageArray[1],
                            messageArray[2], messageArray[3]);

                    System.out.println("Message being sent to" + successorPort);
                    System.out.println("Message is" + msgs[0]);

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (successorPort * 2));
                    socket.setSoTimeout(timeOut);
                    ObjectOutputStream clientOut = new ObjectOutputStream(socket.getOutputStream());
                    clientOut.writeObject(sendMessage);



                    //Close all the sockets and input and output stream
                    clientOut.close();
                    socket.close();


                }catch(Exception e)
                {
                    System.out.println("Socket Timeout happened while waiting for ack from successor");
                    //Means no ACK received -- Get an unique number and save to hashmap
                    e.printStackTrace();
                }

            } //End of type-- replicate
            else if(messageArray[0].equals("Retrieveall"))
            {
                //Make socket connection to get the value from the port and wait for its reply
                // On getting the reply -- Update the value in the queryAck hashmap by calling the function -- saveinALLMAP

                int port= Integer.parseInt(messageArray[1]);
                System.out.println("Port is "+port);

                //Make the message
                CreateMessage sendMessage = new CreateMessage(messageArray[0]);
                try
                {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (port * 2));
                    socket.setSoTimeout(timeOut);
                    ObjectOutputStream clientOut = new ObjectOutputStream(socket.getOutputStream());
                    clientOut.writeObject(sendMessage);

                    System.out.println("getValue Message being sent to port" + port);

                    System.out.println("Waiting for the message");

                    ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                    CreateMessage recvMessage = (CreateMessage) in.readObject();

                    System.out.println("Message reply received ::with message type" + recvMessage.messageType);
                    System.out.println("HashMap received is of size "+recvMessage.sendHashMap.size());

                    saveinAllMap(recvMessage.sendHashMap);

                    //Close all the sockets and input and output stream
                    clientOut.close();
                    in.close();
                    socket.close();

                }catch(Exception e)
                {
                    System.out.println("Socket timeout happened while querying for all data from port "+port);
                    //Ask the data from its successor -- Make a message to send to its successor
                    getAllDatafromSuccessor(messageArray[1]);

                    e.printStackTrace();
                }

            }  //End of message type -- retrieveall
            else if(messageArray[0].equals("resurrection"))
            {
                //Make socket connection to get the value from the port and wait for its reply
                // On getting the reply -- Update the value in the queryAck hashmap by calling the function --saveinRecoveryMap

                int port= Integer.parseInt(messageArray[2]);
                System.out.println("Port is "+port);

                //Make the message - sending as messagetype and key where is the AVD requesting backup

                CreateMessage sendMessage = new CreateMessage(messageArray[0],messageArray[1]);
                try
                {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (port * 2));
                    socket.setSoTimeout(timeOut);
                    ObjectOutputStream clientOut = new ObjectOutputStream(socket.getOutputStream());
                    clientOut.writeObject(sendMessage);

                    System.out.println("getValue Message being sent to port" + port);

                    System.out.println("Waiting for the message");

                    ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                    CreateMessage recvMessage = (CreateMessage) in.readObject();

                    System.out.println("Message reply received ::with message type" + recvMessage.messageType);
                    System.out.println("HashMap received is of size "+recvMessage.sendHashMap.size());

                    saveinRecoveryMap(Integer.toString(port),recvMessage.sendHashMap);


                    //Close all the sockets and input and output stream
                    clientOut.close();
                    in.close();
                    socket.close();

                }catch(Exception e)
                {
                    System.out.println("Socket Timeout happened while trying to recovery querying from "+port);

                    e.printStackTrace();
                }

                System.out.println("End of This function");


            }//End of message type ressurection


            return null;
        }

    }

    public void getAllDatafromSuccessor(String port)
    {
        //Find the successor of the port - Ask for all data with new message type signifying what to ask and then
        //save the data using saveinAllMap

        HashMap<String,String> temp =  finalRing.get(port);
        String successor1 = hashPort.get(temp.get("next1"));
        String successor2 = hashPort.get(temp.get("next2"));

        System.out.println("Successor 1 is "+successor1);
        if(successor1.equals(myPort))
        {
            Cursor reader;
            String getQuery = "SELECT * FROM " + TABLE_NAME + " WHERE avd " + " = '" + port + "';";
            System.out.println("USER:: The get query is " + getQuery);
            reader = readingDb.rawQuery(getQuery, null);


            int keyIndex;
            int valueIndex;
            String returnKey;

            String returnValue;
            for (boolean item = reader.moveToFirst(); item; item = reader.moveToNext()) {

                returnValue = reader.getString(reader.getColumnIndex("value"));
                returnKey = reader.getString(reader.getColumnIndex("key"));

                //Save in global hashMap
                AllReply.put(returnKey, returnValue);
            }
            replyCount++;
        }
        else
        {
            int successor1Port= Integer.parseInt(successor1);


            //Make the message
            //Message type and key
            CreateMessage sendMessage = new CreateMessage("getallforother",port);
            try
            {

                System.out.println("getValue Message being sent to port" + successor1Port);

                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (successor1Port * 2));
                socket.setSoTimeout(timeOut);
                ObjectOutputStream clientOut = new ObjectOutputStream(socket.getOutputStream());
                clientOut.writeObject(sendMessage);


                System.out.println("Waiting for the message");

                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                CreateMessage recvMessage = (CreateMessage) in.readObject();

                System.out.println("Message reply received ::with message type" + recvMessage.messageType);
                System.out.println("HashMap received is of size "+recvMessage.sendHashMap.size());

                saveinAllMap(recvMessage.sendHashMap);

                //Close all the sockets and input and output stream
                clientOut.close();
                in.close();
                socket.close();

            }catch(Exception e)
            {
                System.out.println("Socket timeout happened while querying for all data from port "+port);
                //Ask the data from its successor -- Make a message to send to its successor
                getAllDatafromSuccessor(successor2);

                e.printStackTrace();
            }

        }

    }


    public static class CreateMessage implements Serializable
    {
        String sendPort;
        String message;
        String messageType;
        String originalPort;
        HashMap<String,String> sendHashMap;
        String key;
        String value;
        String returnStatusPort;
        String ackStatus;


        //For sending from original to first replication
        public  CreateMessage(String messageType,String key,String value,String originalPort)
        {
            this.messageType=messageType;
            this.key=key;
            this.value=value;
            this.originalPort=originalPort;

        }


        public  CreateMessage(String messageType,String key,String value,String originalPort,String ackStatus)
        {
            this.messageType=messageType;
            this.key=key;
            this.value=value;
            this.originalPort=originalPort;
            this.ackStatus= ackStatus;
        }

        public  CreateMessage(String messageType,String key)
        {
            this.key=key;
            this.messageType=messageType;
        }

        public  CreateMessage(String messageType,HashMap sendHashMap)
        {
            this.messageType=messageType;
            this.sendHashMap=sendHashMap;
        }

        public  CreateMessage(String messageType)
        {

            this.messageType=messageType;
        }

        public  CreateMessage(String messageType,String key,String value)
        {
            this.key=key;
            this.messageType=messageType;
            this.value=value;
        }

    }




}
