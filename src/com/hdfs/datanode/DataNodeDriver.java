package com.hdfs.datanode;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Enumeration;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hdfs.miscl.Constants;
import com.hdfs.miscl.Hdfs.BlockLocations;
import com.hdfs.miscl.Hdfs.BlockReportRequest;
import com.hdfs.miscl.Hdfs.DataNodeLocation;
import com.hdfs.miscl.Hdfs.WriteBlockRequest;
import com.hdfs.namenode.INameNode;

import static com.hdfs.miscl.Constants.*;


public class DataNodeDriver implements IDataNode {

	public static int id;
	public static int BINDING_PORT;
	
	/**Interface methods start here **/
	public byte[] readBlock(byte[] inp) throws RemoteException {
		// TODO Auto-generated method stub
		System.out.println("Hello");
		return null;
	}


	public byte[] writeBlock(byte[] inp) throws RemoteException {
		// TODO Auto-generated method stub
		System.out.println("In Method write Block");
		byte[] receivedByteArray;
		
		try {
			WriteBlockRequest writeBlockRequestObj = WriteBlockRequest.parseFrom(inp);
			/**Received Byte array **/
			receivedByteArray = writeBlockRequestObj.getData(0).toByteArray();
			/**Block locations object **/
			BlockLocations blockLocObj = writeBlockRequestObj.getBlockInfo();
			
			int blockNumber = blockLocObj.getBlockNumber();
			
			String str = new String(receivedByteArray, StandardCharsets.UTF_8);
			
			/**Write into FIle **/
			FileWriterClass fileWriterObj = new FileWriterClass(blockNumber+"");
			fileWriterObj.createFile();
			fileWriterObj.writeline(str);
			fileWriterObj.closeFile();
			
			/**This is the cascading part **/
			DataNodeLocation dataLocObj = blockLocObj.getLocations(0);
			
			
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
	}

	/**Interface methods end here **/
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		System.out.println("Datanode");
		
		/**Need an argument from command line to uniquely identify the data Node **/
		DataNodeDriver dataDriverObj = new DataNodeDriver();
		id = Integer.parseInt(args[0]);
		
		Registry register = null;
		
		BINDING_PORT = Integer.parseInt(args[0])+DATA_NODE_PORT;
		System.out.println("Binding port is "+BINDING_PORT);

		//Registering an object in java RMI environment
		try
		{
			System.setProperty("java.rmi.server.hostname",getMyIP());
			register = LocateRegistry.createRegistry(BINDING_PORT);
			IDataNode dataStub = (IDataNode) UnicastRemoteObject.exportObject(dataDriverObj,BINDING_PORT);
			

			register.rebind(DATA_NODE_ID,dataStub);
		}
		catch(RemoteException e)
		{
			System.out.println("Remote Exception Caught DataNodeDriverClas  method:main");
		}
		
		blockReportRequest();
		
	}


	private static void blockReportRequest() {
		// TODO Auto-generated method stub
		try {
			Registry register = LocateRegistry.getRegistry(NAME_NODE_IP,Registry.REGISTRY_PORT);
			
			BlockReportRequest.Builder blockRepReqObj  = BlockReportRequest.newBuilder();
			
			
			/**Prepare data node location**/
			DataNodeLocation.Builder dataNodeLocObj = DataNodeLocation.newBuilder();
			dataNodeLocObj.setIp(getMyIP());
			dataNodeLocObj.setPort(BINDING_PORT);
			blockRepReqObj.setLocation(dataNodeLocObj);
			/**Set IP **/
			blockRepReqObj.setId(id);
			
			
			/**DOUBT Figure out what block locations to send **/
			int[] blockNums = new int[3];
			blockNums[0]=1;
			blockNums[1]=2;
			blockNums[2]=3;
			
			for(int i=0;i<3;i++)
				blockRepReqObj.addBlockNumbers(blockNums[i]);
			
			/**Create Stub to call name server methods **/
			INameNode nameNodeStub = (INameNode)register.lookup(NAME_NODE);
			nameNodeStub.blockReport(blockRepReqObj.build().toByteArray());
						
		} catch (RemoteException | NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static String getMyIP()
	{
		String myIp=null;
		Enumeration<NetworkInterface> n = null;
		try {
			n = NetworkInterface.getNetworkInterfaces();
		} catch (SocketException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	     for (; n.hasMoreElements();)
	     {
	             NetworkInterface e = n.nextElement();
//	             System.out.println("Interface: " + e.getName());
	             
	             Enumeration<InetAddress> a = e.getInetAddresses();
	             for (; a.hasMoreElements();)
	             {
	                     InetAddress addr = a.nextElement();
//	                     System.out.println("  " + addr.getHostAddress());
	                     if(e.getName().equals("wlan0"))
	                     {
	                    	myIp = addr.getHostAddress(); 
	                     }
	             }
	     }
	     
	     return myIp;

	}
	
}
