package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.opengl.Matrix;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.ContentValues.TAG;

public class SimpleDynamoProvider extends ContentProvider {

	static final int SERVER_PORT = 10000;
	int msg_count=0;
	String  masterNode ="11108";
	String portStr=null;
	String myPort=null;
	String predecessor=null;
	String successor=null;
	String hashedPort=null;
	TreeSet<String> localChordCopyAtJoin=new TreeSet<String>();
	TreeSet<String> localChordCopyAtQuery=new TreeSet<String>();
	TreeSet<String> localChordCopyAtStarQuery=new TreeSet<String>();
	TreeSet<String> localChordCopyAtDelete=new TreeSet<String>();
	TreeSet<String> chord=new TreeSet<String>();
	TreeSet<String> chordUnHashed=new TreeSet<String>();
	TreeSet<String> localChordData = new TreeSet<String>();
	TreeSet<String> localChordDataUnHashed = new TreeSet<String>();
	Map<String,String> hashAndPortValue = new HashMap<String,String>();
	Map<String,String> localHashAndPortValue = new HashMap<String,String>();
	TreeSet<String> allKeyList = new TreeSet<String>();
	SortedSet<String> localKeyList = Collections.synchronizedSortedSet( new TreeSet<String>());
	String relayCursorKey = null;
	String relayCursorVal= null;
	boolean queryFlag = false;
	String relayCursorKeyVal= null;
	Map<String,String> fileMap = new HashMap<String,String>();
	Map<String,String> rangePort = new HashMap<String,String>();
	Map<String,List<String>> replicaNodes= new HashMap<String, List<String>>();
	TreeSet<String> unHashedList = new TreeSet<String>();
	Map<String,List<String>> SuccPred = new HashMap<String,List<String>>();
	Map<String,List<String>> replicaNodesAtQuery= new HashMap<String, List<String>>();
	Map<String,String> KeyValueInsert = new HashMap<String,String>();
	long time=System.currentTimeMillis();

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub


		if (selection.equals("@")) {

			for (String localKey : localKeyList) {
				localKeyList.remove(localKey);
				getContext().deleteFile(localKey);

			}
			return 0;
		} else if (selection.equals("*")) {
			if (localChordData.size() == 1) {
				for (String localKey : localKeyList) {
					localKeyList.remove(localKey);
					getContext().deleteFile(localKey);

				}
				return 0;
			} else {
				int del = starDelete();
				return del;
			}


		}


		String deleteHashedQueryKey = null;
		String message = null;
		int delete = 0;
		try {
			deleteHashedQueryKey = genHash(selection);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		localChordCopyAtDelete.add(deleteHashedQueryKey);
		Log.e("Chord At delete", localChordCopyAtDelete + "");

		String deleteNode = localChordCopyAtDelete.higher(deleteHashedQueryKey) == null ? localChordCopyAtDelete.first() : localChordCopyAtDelete.higher(deleteHashedQueryKey);
		Log.e("deleteNode", deleteNode);
		localChordCopyAtDelete.remove(deleteHashedQueryKey);
		/*if (hashedPort.equals(deleteNode)) {
			localKeyList.remove(selection);
			getContext().deleteFile(selection);
		}*/
		List<String> deleteNodes = replicaNodes.get(deleteNode);
		for(String delNode : deleteNodes){
			try {

				Socket socket = null;
				/*Creating a new socket connections for each AVDs and multi-casting message to all of them and to itself */

				socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(delNode));
				// Log.e("ClntQueryTask:queryNode",msgs[0]);
				String originalPort = hashedPort;
				String qKey = selection;


				//String finalInsertMsg = "delete" + "-" + originalPort + "-" + qKey;
				Object[] finalInsertMsg = {"delete" , originalPort , qKey};
				/*
				 * TODO: Fill in your client code that sends out a message.
				 */
				ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
				output.writeObject(finalInsertMsg);

				ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
				int keyValue = (Integer) input.readObject();

				// delete = keyValue;


				output.flush();
				socket.close();

			} catch (UnknownHostException e) {
				Log.e(TAG, "ClientTask UnknownHostException");
			} catch (IOException e) {
				Log.e(TAG, "ClientTask socket IOException");
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}


			//Reference-https://developer.android.com/reference/android/database/MatrixCursor#addRow(java.lang.Object[])
			// Log.e("ReturnedRealy", relayCursorVal);
			// relayCursorKey =relayCursorKeyVal.split("-")[0];

		}

		return 0;
	}

	public int starDelete(){

		List<String> allKeys = new ArrayList<String>();
		// allKeys.addAll(allKeyList);
		String  message=null;
		for (String srvrPort : localChordData) {
			try {

				Socket socket = null;
				/*Creating a new socket connections for each AVDs and multi-casting message to all of them and to itself */

				socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(localHashAndPortValue.get(srvrPort)));
				// Log.e("ClntQueryTask:queryNode",msgs[0]);


				String finalInsertMsg = "keyList";

				/*
				 * TODO: Fill in your client code that sends out a message.
				 */
				ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
				output.writeObject(finalInsertMsg);

				ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
				List<String> qkeyLst = (List<String>) input.readObject();

				allKeys.addAll(qkeyLst);
				// Querresult = relayCursorKey + "-"+relayCursorVal;
				Log.e("relayCurser", relayCursorKey + "-" + relayCursorVal);

                   /* DataOutputStream output = new DataOutputStream(socket.getOutputStream());
                    output.writeUTF(msgToSend);

                    DataInputStream input = new DataInputStream(socket.getInputStream());
                    message = (String)input.readUTF();*/

				/*References used*/
				/*https://developer.android.com/reference/android/os/AsyncTask*/
				/*https://developer.android.com/reference/java/io/DataInputStream and https://developer.android.com/reference/java/io/DataOutputStream*/
				/*https://docs.oracle.com/javase/tutorial/networking/sockets/*/
				/*https://stackoverflow.com/questions/28187038/tcp-client-server-program-datainputstream-dataoutputstream-issue*/


				output.flush();
				socket.close();

			} catch (UnknownHostException e) {
				Log.e(TAG, "ClientTask UnknownHostException");
			} catch (IOException e) {
				Log.e(TAG, "ClientTask socket IOException");
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		for(String allKey : allKeys ){
			String deleteHashedQueryKey = null;

			int delete=0;
			try {
				deleteHashedQueryKey = genHash(allKey);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			localChordData.add(deleteHashedQueryKey);
			Log.e("Chord At delete", localChordData + "");

			String deleteNode = localChordData.higher(deleteHashedQueryKey) == null ? localChordData.first() : localChordData.higher(deleteHashedQueryKey);
			Log.e("deleteNode", deleteNode);
			localChordData.remove(deleteHashedQueryKey);
			if (hashedPort.equals(deleteNode)) {
				localKeyList.remove(allKey);
				getContext().deleteFile(allKey);
			}

			else{
				try {

					Socket socket = null;
					/*Creating a new socket connections for each AVDs and multi-casting message to all of them and to itself */

					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(localHashAndPortValue.get(deleteNode)));
					// Log.e("ClntQueryTask:queryNode",msgs[0]);
					String originalPort = hashedPort;
					String qKey = allKey;


					String finalInsertMsg = "delete" + "-" + originalPort + "-" + qKey;

					/*
					 * TODO: Fill in your client code that sends out a message.
					 */
					ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
					output.writeObject(finalInsertMsg);

					ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
					int keyValue = (Integer) input.readObject();

					// delete = keyValue;


					output.flush();
					socket.close();

				} catch (UnknownHostException e) {
					Log.e(TAG, "ClientTask UnknownHostException");
				} catch (IOException e) {
					Log.e(TAG, "ClientTask socket IOException");
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}


				//Reference-https://developer.android.com/reference/android/database/MatrixCursor#addRow(java.lang.Object[])
				// Log.e("ReturnedRealy", relayCursorVal);
				// relayCursorKey =relayCursorKeyVal.split("-")[0];

			}

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
				time=System.currentTimeMillis();
		Log.e("SuccAndpred", predecessor+":"+successor);
		Log.e("Add to node Before",hashedPort);
		OutputStream f_0utStrm;
		String fileName=values.getAsString("key");
		String fileValue=values.getAsString("value");
		Log.e("File Val",fileValue);
		Log.e("Add to node After",hashedPort);
		try {
			String hashedfileName =genHash(fileName);
			Log.e("hashedfileName",hashedfileName);
			Log.e("Chord Size Insert",localChordData.size()+"");
			if(localChordData.size()==0){
				localChordData.add(hashedPort);
				localHashAndPortValue.put(hashedPort,myPort);
			}
			localChordData.add(hashedfileName);
			String insertNode= localChordData.higher(hashedfileName)==null?localChordData.first():localChordData.higher(hashedfileName);
			localChordData.remove(hashedfileName);
			Log.e("Add to node",localHashAndPortValue.get(insertNode));
			allKeyList.add(fileName);
			List<String> replicate = replicaNodes.get(insertNode);
			for(String insNode : replicate) {

				try {

					Socket socket=null;
					/*Creating a new socket connections for each AVDs and multi-casting message to all of them and to itself */

					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(insNode));
					Log.e("Inserting Key-Val", fileName + "-"+ fileValue +"-"+insertNode);

					//String  finalInsertMsg = "insert"+"-"+fileName + "-" +fileValue;
					Object[] finalInsertMsg = {"insert",fileName,fileValue,time};
					/*
					 * TODO: Fill in your client code that sends out a message.
					 */
					ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
					output.writeObject(finalInsertMsg);

					ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
					String msg = (String)input.readObject();


                   /* DataOutputStream output = new DataOutputStream(socket.getOutputStream());
                    output.writeUTF(msgToSend);

                    DataInputStream input = new DataInputStream(socket.getInputStream());
                    message = (String)input.readUTF();*/

					/*References used*/
					/*https://developer.android.com/reference/android/os/AsyncTask*/
					/*https://developer.android.com/reference/java/io/DataInputStream and https://developer.android.com/reference/java/io/DataOutputStream*/
					/*https://docs.oracle.com/javase/tutorial/networking/sockets/*/
					/*https://stackoverflow.com/questions/28187038/tcp-client-server-program-datainputstream-dataoutputstream-issue*/



					output.flush();
					socket.close();

				} catch (UnknownHostException e) {
					Log.e(TAG, "ClientTask UnknownHostException");
				} catch (IOException e) {
					Log.e(TAG, "ClientTask socket IOException");
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
			}
		} catch (Exception e) {
			Log.e(TAG, "File write failed");
		}


		return uri;
	}


	@Override
	public boolean onCreate() {
		/*for(String fln : getContext().fileList()) {
			getContext().deleteFile(fln);
		}*/

		Log.e("onCreateFileList",getContext().fileList().toString());

		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));

		replicaNodes.put("177ccecaec32c54b82d5aaafc18a2dadb753e3b1", Arrays.asList("11124","11112","11108"));
		replicaNodes.put("208f7f72b198dadd244e61801abe1ec3a4857bc9", Arrays.asList("11112","11108","11116"));
		replicaNodes.put("33d6357cfaaf0f72991b0ecd8c56da066613c089", Arrays.asList("11108","11116","11120"));
		replicaNodes.put("abf0fd8db03e5ecb199a9b82929e9db79b909643", Arrays.asList("11116","11120","11124"));
		replicaNodes.put("c25ddd596aa7c81fa12378fa725f706d54325d12", Arrays.asList("11120","11124","11112"));
		rangePort.put("177ccecaec32c54b82d5aaafc18a2dadb753e3b1","33d6357cfaaf0f72991b0ecd8c56da066613c089" );
		rangePort.put("208f7f72b198dadd244e61801abe1ec3a4857bc9", "abf0fd8db03e5ecb199a9b82929e9db79b909643");
		rangePort.put("33d6357cfaaf0f72991b0ecd8c56da066613c089","c25ddd596aa7c81fa12378fa725f706d54325d12");
		rangePort.put("abf0fd8db03e5ecb199a9b82929e9db79b909643", "177ccecaec32c54b82d5aaafc18a2dadb753e3b1");
		rangePort.put("c25ddd596aa7c81fa12378fa725f706d54325d12", "208f7f72b198dadd244e61801abe1ec3a4857bc9");
		// new SimpleDhtActivity().onCreate(new Bundle());
		SuccPred.put("177ccecaec32c54b82d5aaafc18a2dadb753e3b1", Arrays.asList("c25ddd596aa7c81fa12378fa725f706d54325d12","208f7f72b198dadd244e61801abe1ec3a4857bc9"));
		SuccPred.put("208f7f72b198dadd244e61801abe1ec3a4857bc9", Arrays.asList("177ccecaec32c54b82d5aaafc18a2dadb753e3b1","33d6357cfaaf0f72991b0ecd8c56da066613c089"));
		SuccPred.put("33d6357cfaaf0f72991b0ecd8c56da066613c089", Arrays.asList("208f7f72b198dadd244e61801abe1ec3a4857bc9","abf0fd8db03e5ecb199a9b82929e9db79b909643"));
		SuccPred.put("abf0fd8db03e5ecb199a9b82929e9db79b909643", Arrays.asList("33d6357cfaaf0f72991b0ecd8c56da066613c089","c25ddd596aa7c81fa12378fa725f706d54325d12"));
		SuccPred.put("c25ddd596aa7c81fa12378fa725f706d54325d12", Arrays.asList("abf0fd8db03e5ecb199a9b82929e9db79b909643","177ccecaec32c54b82d5aaafc18a2dadb753e3b1"));

		localChordData.add("177ccecaec32c54b82d5aaafc18a2dadb753e3b1");
		localChordData.add("208f7f72b198dadd244e61801abe1ec3a4857bc9");
		localChordData.add("33d6357cfaaf0f72991b0ecd8c56da066613c089");
		localChordData.add("abf0fd8db03e5ecb199a9b82929e9db79b909643");
		localChordData.add("c25ddd596aa7c81fa12378fa725f706d54325d12");
		localHashAndPortValue.put("177ccecaec32c54b82d5aaafc18a2dadb753e3b1","11124");
		localHashAndPortValue.put("208f7f72b198dadd244e61801abe1ec3a4857bc9","11112");
		localHashAndPortValue.put("33d6357cfaaf0f72991b0ecd8c56da066613c089","11108");
		localHashAndPortValue.put("abf0fd8db03e5ecb199a9b82929e9db79b909643","11116");
		localHashAndPortValue.put("c25ddd596aa7c81fa12378fa725f706d54325d12","11120");
		 localChordCopyAtJoin.addAll(localChordData);
		 localChordCopyAtQuery.addAll(localChordData);
		 localChordCopyAtStarQuery.addAll(localChordData);
		localChordCopyAtDelete.addAll(localChordData);

		replicaNodesAtQuery.putAll(replicaNodes);

		try {
			hashedPort = genHash(portStr);

			Log.e("hash and its Port",hashedPort +":"+ myPort);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		try {

			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e("InsideOncrate", "Can't create a ServerSocket");
			return  false;
		}

			Log.e("Join request",hashedPort +":"+ myPort);
			new ClientJoinTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,hashedPort, masterNode,myPort);

		return true;

	}

	private class ClientJoinTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void  doInBackground(String... msgs) {


			String rangeHash = rangePort.get(hashedPort);
			//getContext().deleteFile()


			for(String fln : getContext().fileList()) {
				getContext().deleteFile(fln);
			}
			/*for(String fln : getContext().getFilesDir().list()) {
				getContext().deleteFile(fln);
			}*/
			List<String> sp = SuccPred.get(hashedPort);
			SortedSet<String> qkeyLst=new TreeSet<String>();
			SortedSet<String> qkeyLstSp=new TreeSet<String>();
			TreeSet<String> allExptHost = new TreeSet<String>();
			allExptHost.addAll(localChordData);
			allExptHost.remove(hashedPort);
			TreeSet<String> copyKeyList=new TreeSet<String>();
			Map<String,String> keyValMap = new HashMap<String, String>();
			Map<String,String> keyValMapAll = new HashMap<String, String>();
			for (String spList : allExptHost) {
				try {
					//List<String> indList = new ArrayList<String>();
					Socket socket = null;
					/*Creating a new socket connections for each AVDs and multi-casting message to all of them and to itself */

					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(localHashAndPortValue.get(spList)));
					Object[] finalInsertMsg = {"keyValue"};
					/*
					 * TODO: Fill in your client code that sends out a message.
					 */
					ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
					output.writeObject(finalInsertMsg);

					ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
					Object[] qkeyLstkeyVal = (Object[]) input.readObject();
					qkeyLstSp.addAll((SortedSet<String>)qkeyLstkeyVal[0]);
					keyValMap =(Map<String, String>) qkeyLstkeyVal[1];
					for(String key :keyValMap.keySet() ) {
						if(keyValMapAll.containsKey(key)) {
							long x=Long.valueOf(keyValMapAll.get(key).split("-")[1]);
							long y=Long.valueOf(keyValMap.get(key).split("-")[1]);
							if(x<y)
							keyValMapAll.put(key,keyValMap.get(key));
						}
						else keyValMapAll.put(key,keyValMap.get(key));
					}
					output.flush();
					socket.close();
					//input.close();

				} catch (UnknownHostException e) {
					Log.e(TAG, "ClientTask UnknownHostException");
				} catch (IOException e) {
					Log.e(TAG, "ClientTask socket IOException");
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
			}

			String hashedQueryKey = null;
			for(String qureryfor : qkeyLstSp) {
				try {
					 hashedQueryKey = genHash(qureryfor);
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}
				localChordCopyAtJoin.add(hashedQueryKey);
				Log.e("Chord After fileNInsert",localChordCopyAtJoin+"");
				// Log.e("Chord fileNInsertNNxxt",localChordData.higher(hashedfileName)+"");
				String insertNode= localChordCopyAtJoin.higher(hashedQueryKey)==null?localChordCopyAtJoin.first():localChordCopyAtJoin.higher(hashedQueryKey);
				//  Log.e("NNNNN",localChordData.higher(hashedfileName));
				localChordCopyAtJoin.remove(hashedQueryKey);


				if (!(insertNode.equals(rangeHash) || insertNode.equals(sp.get(1)))) {

					copyKeyList.add(qureryfor);

                    String relayKeyExc;
                    String relayValExc ;
                    Socket socket = null;
                    localKeyList.add(qureryfor);
					KeyValueInsert.put(qureryfor,keyValMapAll.get(qureryfor));
					try {
						OutputStream f_0utStrm = getContext().openFileOutput(qureryfor, Context.MODE_PRIVATE);
						f_0utStrm.write((keyValMapAll.get(qureryfor)).getBytes());
						f_0utStrm.close();
					} catch (FileNotFoundException e1) {
						e1.printStackTrace();
					} catch (IOException e1) {
						e1.printStackTrace();
					}

				}

			}


			return null;
		}
	}



	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			Object[] remoteMessage;

			/*
			 * TODO: Fill in your server code that receives messages and passes them
			 * to onProgressUpdate().
			 */

			try {

				while (true) {
					Socket socket = serverSocket.accept();

					ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
					remoteMessage = (Object[])input.readObject();
					//publishProgress(message);

					//String[] remoteMessage = message.split("-");
					String ReqMsg = (String) remoteMessage[0];


					ObjectOutputStream output = null;
					Log.e("Join req in server", hashedPort + ":" + myPort);
					Log.e("Chord server before", ":" + chord);
					if (ReqMsg.equals("join")) {

						output = new ObjectOutputStream(socket.getOutputStream());
						Object[] allaData = {chord, localHashAndPortValue,unHashedList,localChordData};
						output.writeObject(allaData);

						Log.e("Chord server After", ":" + chord);
					} else if (ReqMsg.equals("insert")) {
						KeyValueInsert.put((String)remoteMessage[1],(String)remoteMessage[2]+"-"+(Long)remoteMessage[3]);
						localKeyList.add((String)remoteMessage[1]);
						OutputStream f_0utStrm = getContext().openFileOutput((String)remoteMessage[1], Context.MODE_PRIVATE);
						f_0utStrm.write(((String)remoteMessage[2] +"-"+(Long)remoteMessage[3]).getBytes());
						output = new ObjectOutputStream(socket.getOutputStream());
						output.writeObject("InsertDone");
						f_0utStrm.close();
						Log.e("InsertingInserver", (String)remoteMessage[1]+"-"+ ((String)remoteMessage[2]).getBytes());
					}  else if (ReqMsg.equals("query")) {
						String message=null;
						try {
							Log.e("InsidequeryServer", (String)remoteMessage[2]);

							/*Reference-https://developer.android.com/reference/android/content/Context#openFileInput(java.lang.String)*/
							InputStream f_inStrm = getContext().openFileInput((String)remoteMessage[2]);
							/*Reference-https://stackoverflow.com/questions/2864117/read-data-from-a-text-file-using-java*/
							BufferedReader br = new BufferedReader(new InputStreamReader(f_inStrm));
							message = br.readLine();
							f_inStrm.close();
							Log.e("InsidequeryServerWritng", remoteMessage[2] + "-" + message);
						} catch (FileNotFoundException e) {
							e.printStackTrace();
						} catch (IOException e) {
							e.printStackTrace();
						}


						/*
						 * MatrixCursor takes column names as the constructor arguments*/
						/*Reference-https://developer.android.com/reference/android/database/MatrixCursor#MatrixCursor(java.lang.String[])*/

						output = new ObjectOutputStream(socket.getOutputStream());
						output.writeObject(remoteMessage[2] + "-" + message);
						queryFlag = true;
						Log.e("InsidequeryServer", "wrote Cursor");
					}

					else if (ReqMsg.equals("queryHashMap")) {
						String message=null;
						try {
							Log.e("InsidequeryServer", (String)remoteMessage[2]);

							/*Reference-https://developer.android.com/reference/android/content/Context#openFileInput(java.lang.String)*/
							InputStream f_inStrm = getContext().openFileInput((String)remoteMessage[2]);
							/*Reference-https://stackoverflow.com/questions/2864117/read-data-from-a-text-file-using-java*/
							BufferedReader br = new BufferedReader(new InputStreamReader(f_inStrm));
							message = br.readLine();
							Object[] keyValTime = {remoteMessage[2],KeyValueInsert.get(remoteMessage[2])};
							output = new ObjectOutputStream(socket.getOutputStream());
							output.writeObject(keyValTime);
							f_inStrm.close();
							Log.e("InsidequeryServerWritng", remoteMessage[2] + "-" + message);
						} catch (FileNotFoundException e) {
							Object[] keyValTime = {remoteMessage[2],"null"};
							output = new ObjectOutputStream(socket.getOutputStream());
							output.writeObject(keyValTime);
						} catch (IOException e) {
							Object[] keyValTime = {remoteMessage[2],"null"};
							output = new ObjectOutputStream(socket.getOutputStream());
							output.writeObject(keyValTime);
						} catch (NullPointerException e){
							Object[] keyValTime = {remoteMessage[2],"null"};
							output = new ObjectOutputStream(socket.getOutputStream());
							output.writeObject(keyValTime);
						}


						/*
						 * MatrixCursor takes column names as the constructor arguments*/
						/*Reference-https://developer.android.com/reference/android/database/MatrixCursor#MatrixCursor(java.lang.String[])*/
						/*Object[] keyValTime = {remoteMessage[2],KeyValueInsert.get(remoteMessage[2])};
						output = new ObjectOutputStream(socket.getOutputStream());
						output.writeObject(keyValTime);*/
						queryFlag = true;
						Log.e("InsidequeryServer", "wrote Cursor");
					}


					else if(ReqMsg.equals("keyList")){
						Log.e("keyListServer", "wrote keyList");
						output = new ObjectOutputStream(socket.getOutputStream());
						output.writeObject(localKeyList);
						queryFlag = true;

					}
					else if(ReqMsg.equals("keyValue")){
						Log.e("keyListServer", "wrote keyList");
						output = new ObjectOutputStream(socket.getOutputStream());
						Object[] keyListKeyValMap = {localKeyList,KeyValueInsert};
						output.writeObject(keyListKeyValMap);
						queryFlag = true;

					}
					else if(ReqMsg.equals("delete")){
						localKeyList.remove(remoteMessage[2]);
						getContext().deleteFile((String)remoteMessage[2]);
						output = new ObjectOutputStream(socket.getOutputStream());
						output.writeObject(1);
					}



					output.flush();
					socket.close();

					/*References used*/
					/*https://developer.android.com/reference/android/os/AsyncTask*/
					/*https://developer.android.com/reference/java/io/DataInputStream and https://developer.android.com/reference/java/io/DataOutputStream*/
					/*https://docs.oracle.com/javase/tutorial/networking/sockets/*/
					/*https://stackoverflow.com/questions/28187038/tcp-client-server-program-datainputstream-dataoutputstream-issue*/


				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (Exception ex) {
				ex.printStackTrace();
			}


			return null;
		}
	}



	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
						String sortOrder) {

		InputStream f_inStrm;
		String message = "";
		String qPort = myPort;
		String hashedQueryKey = null;

	SortedSet<String> localKeyListCopy = new TreeSet<String>();
		if (selection.equals("@")) {
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
				Log.e("Inside@","BeforeFilelist");
				for (String localKey : KeyValueInsert.keySet()) {

					try {
						hashedQueryKey = genHash(localKey);
					} catch (NoSuchAlgorithmException e) {
						e.printStackTrace();
					}
					localChordCopyAtQuery.add(hashedQueryKey);
					Log.e("Chord At query", localChordCopyAtQuery + "");
					// Log.e("Chord fileNInsertNNxxt",localChordData.higher(hashedfileName)+"");
					String queryNode = localChordCopyAtQuery.higher(hashedQueryKey) == null ? localChordCopyAtQuery.first() : localChordCopyAtQuery.higher(hashedQueryKey);
					Log.e("queryNode", queryNode);
					localChordCopyAtQuery.remove(hashedQueryKey);
					//synchAllWrites(queryNode,SuccPred,replicaNodes.get(queryNode).get(2));
					List<String> queryNodes = replicaNodesAtQuery.get(queryNode);

					Map<String,String> qmap = new HashMap<String, String>();
					for (String qNode : queryNodes) {
						try {

							Socket socket = null;
							/*Creating a new socket connections for each AVDs and multi-casting message to all of them and to itself */

							socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt((qNode)));
							// Log.e("ClntQueryTask:queryNode",msgs[0]);
							String originalPort = hashedPort;
							String qKey = localKey;

							Log.e("QueryKey-Node-Loop",selection+"-"+qNode);
							//String  finalInsertMsg = "query"+"-"+originalPort+"-"+qKey;
							//Object[] finalInsertMsg = {"query", originalPort, qKey};
							Object[] finalInsertMsg = {"queryHashMap", originalPort, qKey};
							/*
							 * TODO: Fill in your client code that sends out a message.
							 */
							ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
							output.writeObject(finalInsertMsg);
							//Thread.sleep(500);
							//socket.setSoTimeout(500);
							ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
							Object[] keyValue = (Object[]) input.readObject();

							String relayKey = (String) keyValue[0];
							String relayVal = (String)keyValue[1];
							//String relayTime = keyValue.split("-")[2];
							Log.e("relayCurser", relayVal+ "");
							if(!relayVal.equals("null")&&!relayVal.equals(null)) {
								if (qmap.containsKey(relayKey)) {
									long x = Long.valueOf(qmap.get(relayKey).split("-")[1]);
									long y = Long.valueOf(relayVal.split("-")[1]);
									if (x < y) {
										qmap.put(relayKey, relayVal);
									}
								} else qmap.put(relayKey, relayVal);
							}

							output.flush();
							socket.close();

						} catch (UnknownHostException e) {
							//continue;
						} catch (IOException e) {
							//continue;
						} catch (ClassNotFoundException e) {
							//continue;
						}

					}


					Log.e("K-V", localKey+"-"+ KeyValueInsert.get(localKey).split("-")[0]);
					/*
					 * MatrixCursor takes column names as the constructor arguments*/
					/*Reference-https://developer.android.com/reference/android/database/MatrixCursor#MatrixCursor(java.lang.String[])*/
					//MatrixCursor cursor = new MatrixCursor(new String[] {"key", "value"});
					/*Reference-https://developer.android.com/reference/android/database/MatrixCursor#addRow(java.lang.Object[])*/

					cursor.addRow(new String[]{localKey, qmap.get(localKey).split("-")[0]});
				}


			return cursor;

		}

		if (selection.equals("*")) {

				MatrixCursor cursor = startQuery();
				return cursor;

		}


		try {
			hashedQueryKey = genHash(selection);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}


		localChordCopyAtQuery.add(hashedQueryKey);
		Log.e("Chord At query", localChordCopyAtQuery + "");
		// Log.e("Chord fileNInsertNNxxt",localChordData.higher(hashedfileName)+"");
		String queryNode = localChordCopyAtQuery.higher(hashedQueryKey) == null ? localChordCopyAtQuery.first() : localChordCopyAtQuery.higher(hashedQueryKey);
		Log.e("queryNode", queryNode);
		localChordCopyAtQuery.remove(hashedQueryKey);
		//synchAllWrites(queryNode,SuccPred,replicaNodes.get(queryNode).get(2));
		List<String> queryNodes = replicaNodesAtQuery.get(queryNode);
		MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
      Log.e("QueryKey-Node",selection+"-"+queryNode);
		String k =null;
		String v = null;
		Map<String,String> qmap = new HashMap<String, String>();
			for (String qNode : queryNodes) {
				try {

					Socket socket = null;
					/*Creating a new socket connections for each AVDs and multi-casting message to all of them and to itself */

					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt((qNode)));
					// Log.e("ClntQueryTask:queryNode",msgs[0]);
					String originalPort = hashedPort;
					String qKey = selection;

					Log.e("QueryKey-Node-Loop",selection+"-"+qNode);
					//String  finalInsertMsg = "query"+"-"+originalPort+"-"+qKey;
					//Object[] finalInsertMsg = {"query", originalPort, qKey};
					Object[] finalInsertMsg = {"queryHashMap", originalPort, qKey};
					/*
					 * TODO: Fill in your client code that sends out a message.
					 */
					ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
					output.writeObject(finalInsertMsg);
					//Thread.sleep(500);
					//socket.setSoTimeout(500);
					ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
					Object[] keyValue = (Object[]) input.readObject();

                    String relayKey = (String) keyValue[0];
                    String relayVal = (String)keyValue[1];
					//String relayTime = keyValue.split("-")[2];
					Log.e("relayCurser", relayVal+ "");
					if(!relayVal.equals("null")&&!relayVal.equals(null)) {
						if (qmap.containsKey(relayKey)) {
							long x = Long.valueOf(qmap.get(relayKey).split("-")[1]);
							long y = Long.valueOf(relayVal.split("-")[1]);
							if (x < y) {
								qmap.put(relayKey, relayVal);
							}
						} else qmap.put(relayKey, relayVal);
					}

					/*References used*/
					/*https://developer.android.com/reference/android/os/AsyncTask*/
					/*https://developer.android.com/reference/java/io/DataInputStream and https://developer.android.com/reference/java/io/DataOutputStream*/
					/*https://docs.oracle.com/javase/tutorial/networking/sockets/*/
					/*https://stackoverflow.com/questions/28187038/tcp-client-server-program-datainputstream-dataoutputstream-issue*/


					output.flush();
					socket.close();

				} catch (UnknownHostException e) {
					//continue;
				} catch (IOException e) {
					//continue;
				} catch (ClassNotFoundException e) {
					//continue;
				}

			}

		// =Log.e("ReturnedRealy", relayCursorVal);
		cursor.addRow(new String[]{selection, qmap.get(selection).split("-")[0]});
	 return  cursor;
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

	public  MatrixCursor startQuery(){

		SortedSet<String> allKeys = new TreeSet<String>();
		// allKeys.addAll(allKeyList);
		String  message=null;
		for (String srvrPort : localChordData) {
			try {

				Socket socket = null;
				/*Creating a new socket connections for each AVDs and multi-casting message to all of them and to itself */

				socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(localHashAndPortValue.get(srvrPort)));
				// Log.e("ClntQueryTask:queryNode",msgs[0]);


				//String finalInsertMsg = "keyList";
				Object[] finalInsertMsg = {"keyList"};
				/*
				 * TODO: Fill in your client code that sends out a message.
				 */
				ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
				output.writeObject(finalInsertMsg);
				//Thread.sleep(500);
				//socket.setSoTimeout(500);
				ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
				SortedSet<String> qkeyLst = (SortedSet<String>) input.readObject();

				allKeys.addAll(qkeyLst);
				// Querresult = relayCursorKey + "-"+relayCursorVal;
				Log.e("relayCurser", relayCursorKey + "-" + relayCursorVal);

                   /* DataOutputStream output = new DataOutputStream(socket.getOutputStream());
                    output.writeUTF(msgToSend);

                    DataInputStream input = new DataInputStream(socket.getInputStream());
                    message = (String)input.readUTF();*/

				/*References used*/
				/*https://developer.android.com/reference/android/os/AsyncTask*/
				/*https://developer.android.com/reference/java/io/DataInputStream and https://developer.android.com/reference/java/io/DataOutputStream*/
				/*https://docs.oracle.com/javase/tutorial/networking/sockets/*/
				/*https://stackoverflow.com/questions/28187038/tcp-client-server-program-datainputstream-dataoutputstream-issue*/


				output.flush();
				socket.close();

			} catch (UnknownHostException e) {
				Log.e(TAG, "ClientTask UnknownHostException");
			} catch (IOException e) {
				Log.e(TAG, "ClientTask socket IOException");
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
		for (String allk : allKeys) {
			String starHashedQueryKey = null;
			try {
				starHashedQueryKey = genHash(allk);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			localChordCopyAtStarQuery.add(starHashedQueryKey);
			Log.e("Chord At query", localChordData + "");
			// Log.e("Chord fileNInsertNNxxt",localChordData.higher(hashedfileName)+"");
			String starQueryNode = localChordCopyAtStarQuery.higher(starHashedQueryKey) == null ? localChordCopyAtStarQuery.first() : localChordCopyAtStarQuery.higher(starHashedQueryKey);
			Log.e("queryNode", starQueryNode);
			localChordCopyAtStarQuery.remove(starHashedQueryKey);
			List<String> queryNodes = replicaNodes.get(starQueryNode);


			Map<String,String> qmap = new HashMap<String, String>();
			for (String qNode : queryNodes) {
				try {

					Socket socket = null;
					/*Creating a new socket connections for each AVDs and multi-casting message to all of them and to itself */

					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt((qNode)));
					// Log.e("ClntQueryTask:queryNode",msgs[0]);
					String originalPort = hashedPort;
					String qKey = allk;

					Log.e("QueryKey-Node-Loop",allk+"-"+qNode);
					//String  finalInsertMsg = "query"+"-"+originalPort+"-"+qKey;
					//Object[] finalInsertMsg = {"query", originalPort, qKey};
					Object[] finalInsertMsg = {"queryHashMap", originalPort, qKey};
					/*
					 * TODO: Fill in your client code that sends out a message.
					 */
					ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
					output.writeObject(finalInsertMsg);
					//Thread.sleep(500);
					//socket.setSoTimeout(500);
					ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
					Object[] keyValue = (Object[]) input.readObject();

					String relayKey = (String) keyValue[0];
					String relayVal = (String)keyValue[1];
					//String relayTime = keyValue.split("-")[2];
					Log.e("relayCurser", relayVal+ "");
					if(!relayVal.equals("null")&&!relayVal.equals(null)) {
						if (qmap.containsKey(relayKey)) {
							long x = Long.valueOf(qmap.get(relayKey).split("-")[1]);
							long y = Long.valueOf(relayVal.split("-")[1]);
							if (x < y) {
								qmap.put(relayKey, relayVal);
							}
						} else qmap.put(relayKey, relayVal);
					}


					/*References used*/
					/*https://developer.android.com/reference/android/os/AsyncTask*/
					/*https://developer.android.com/reference/java/io/DataInputStream and https://developer.android.com/reference/java/io/DataOutputStream*/
					/*https://docs.oracle.com/javase/tutorial/networking/sockets/*/
					/*https://stackoverflow.com/questions/28187038/tcp-client-server-program-datainputstream-dataoutputstream-issue*/


					output.flush();
					socket.close();

				} catch (UnknownHostException e) {
					//continue;
				} catch (IOException e) {
					//continue;
				} catch (ClassNotFoundException e) {
					//continue;
				}

			}

			// =Log.e("ReturnedRealy", relayCursorVal);
			cursor.addRow(new String[]{allk, qmap.get(allk).split("-")[0]});

		}
		return  cursor;
	}
}
