package com.hdfs.client;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;
import java.io.*;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.hdfs.datanode.IDataNode;
import com.hdfs.miscl.Constants;
import com.hdfs.miscl.Hdfs.AssignBlockRequest;
import com.hdfs.miscl.Hdfs.AssignBlockResponse;
import com.hdfs.miscl.Hdfs.BlockLocations;
import com.hdfs.miscl.Hdfs.CloseFileRequest;
import com.hdfs.miscl.Hdfs.CloseFileResponse;
import com.hdfs.miscl.Hdfs.DataNodeLocation;
import com.hdfs.miscl.Hdfs.OpenFileRequest;
import com.hdfs.miscl.Hdfs.OpenFileResponse;
import com.hdfs.miscl.Hdfs.WriteBlockRequest;
import com.hdfs.namenode.INameNode;

public class ClientDriver {

	public static String fileName;
	public static boolean getOrPutFlag;//true for read, false for write
	public static int fileHandle;
	public static byte[] byteArray;
	public static FileInputStream fis;
	public static long FILESIZE;
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
//		bindToRegistry();
//		System.exit(0);
		/**Allocating 32MB of memory to byteArray **/
//		byteArray = new byte[(int)Constants.BLOCK_SIZE];
		
		fileName = args[0];
		
		/**args[1] can be get put or list **/
		if(args[1].toLowerCase().equals("put"))
		{		
			openFilePut();
		}
		else if(args[1].toLowerCase().equals("get"))
		{			
			openFileGet();
		}
		else if(args[1].toLowerCase().equals("list"))
		{
			//Calls the list method of the name node server
		}
			

				
	}
	
	
	
	/**Open file request method 
	 * Here the filename is obtained from the command line
	 * Along with that a flag is also passed telling whether it is a read request or a write request
	 * **/
	public static void openFileGet()
	{
		OpenFileRequest.Builder openFileReqObj = OpenFileRequest.newBuilder();
		openFileReqObj.setFileName(fileName);		
		openFileReqObj.setForRead(true);
		
	}
	
	
	/**Put Request from Client to name node **/
	public static void openFilePut()
	{
		
		byte[] responseArray;
		
		OpenFileRequest.Builder openFileReqObj = OpenFileRequest.newBuilder();
		openFileReqObj.setFileName(fileName);
		openFileReqObj.setForRead(false);		
		
		try 
		{			
			Registry registry=LocateRegistry.getRegistry(Constants.NAME_NODE_IP,Registry.REGISTRY_PORT);
			INameNode nameStub;
			int status;
			
				try 
				{
					nameStub = (INameNode) registry.lookup(Constants.NAME_NODE);
					responseArray = nameStub.openFile(openFileReqObj.build().toByteArray());
					
					/**The response Array will contain the FileHandle status and the block numbers **/
					
					OpenFileResponse responseObj = OpenFileResponse.parseFrom(responseArray);
					
					fileHandle = responseObj.getHandle();
					System.out.println("The file handle is "+fileHandle);
					
					status = responseObj.getStatus();
					if(status==Constants.STATUS_NOT_FOUND)//status failed change it
					{
						System.out.println("Fatal Error!");
						System.exit(0);
					}
					
					AssignBlockRequest.Builder assgnBlockReqObj = AssignBlockRequest.newBuilder(); 
					
					
					/**required variables **/

					
					int offset=0;
					
					/**calculate block size **/
					int no_of_blocks=getNumberOfBlocks();					
					try {
						/**open the input stream **/
						fis = new FileInputStream(fileName);
					} catch (FileNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

					
					/**FOR LOOP STARTS HERE **/
					for(int i=0;i<no_of_blocks;i++)
					{
						WriteBlockRequest.Builder writeBlockObj = WriteBlockRequest.newBuilder();
						AssignBlockResponse assignResponseObj ;
						BlockLocations blkLocation ;
						List<DataNodeLocation> dataNodeLocations;
						DataNodeLocation dataNode;
						/**need to call assign block and write blocks **/
						
						assgnBlockReqObj.setHandle(fileHandle);
						
						/**Calling assign block **/
						responseArray = nameStub.assignBlock(assgnBlockReqObj.build().toByteArray());
						
						assignResponseObj = AssignBlockResponse.parseFrom(responseArray);
						
						status = assignResponseObj.getStatus();
						if(status==Constants.STATUS_FAILED)
						{
							System.out.println("Fatal Error!");
							System.exit(0);
						}
						
						blkLocation = assignResponseObj.getNewBlock();
						
						int blockNumber = blkLocation.getBlockNumber();
						System.out.println("Block number retured is "+blockNumber);
						
						dataNodeLocations = blkLocation.getLocationsList();
						
						dataNode = dataNodeLocations.get(0);
//						dataNodeLocations.remove(0);
						
						
						Registry registry2=LocateRegistry.getRegistry(dataNode.getIp(),dataNode.getPort());

						System.out.println(dataNode);
						IDataNode dataStub = (IDataNode) registry2.lookup(Constants.DATA_NODE_ID);
						dataStub.readBlock(null);
						
						System.out.println("Control enters here");
						/**read 32MB from file, send it as bytes, this fills in the byteArray**/
						
						byte[] byteArray = read32MBfromFile(offset);
						offset=offset+(int)Constants.BLOCK_SIZE;
						
						
//						BlockLocations.Builder block_send = BlockLocations.newBuilder();
//						block_send.setBlockNumber(blkLocation.getBlockNumber());
//						
//						if(blkLocation.getLocationsCount()==2)
//							block_send.addLocations(blkLocation.getLocations(1));
						
//						writeBlockObj.setBlockInfo(block_send);
//						writeBlockObj.setData(0, ByteString.copyFrom(byteArray));
						writeBlockObj.addData(ByteString.copyFrom(byteArray));
						writeBlockObj.setBlockInfo(blkLocation);
						
						dataStub.writeBlock(writeBlockObj.build().toByteArray());
//						byteArray = null;
						

						
					}
					
					CloseFileRequest.Builder closeFileObj = CloseFileRequest.newBuilder();
					closeFileObj.setHandle(fileHandle);
					
					byte[] receivedArray = nameStub.closeFile(closeFileObj.build().toByteArray());
					CloseFileResponse closeResObj = CloseFileResponse.parseFrom(receivedArray);
					if(closeResObj.getStatus()==Constants.STATUS_FAILED)
					{
						System.out.println("Close File response Status Failed");
						System.exit(0);
					}
					
					try {
						/**Close the input Stream **/
						fis.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
				}
				catch (NotBoundException | InvalidProtocolBufferException e) {
					// TODO Auto-generated catch block
					System.out.println("Could not find NameNode");
					e.printStackTrace();
				}
				
			
		}catch (RemoteException e) {
			// TODO Auto-generated catch block
				e.printStackTrace();
		}		
		
	}

	
	public static int getNumberOfBlocks()
	{
		File inputFile = new File(fileName);
		if(!inputFile.exists())
		{
			System.out.println("File Does not exist");
			System.exit(0);
		}
		
		long fileSize = inputFile.length();
		FILESIZE=inputFile.length();
		double noOfBlocks = Math.ceil(fileSize/Constants.BLOCK_SIZE);
		
		
		return (int)noOfBlocks;
	}
	
	/**Read 32MB size of data from the provided input file **/
	public static byte[] read32MBfromFile(int offset)
	{
		
		System.out.println("offset is "+offset);

		
		BufferedReader breader = null;
		try {
			breader = new BufferedReader(new FileReader(fileName) );
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		

		
		
		int bytesToBeRead = (int)Constants.BLOCK_SIZE;
		
		int limit =offset+(int)Constants.BLOCK_SIZE; 
		
		if(limit >= (int) FILESIZE)
		{
			bytesToBeRead = (int)FILESIZE - offset;
		}
		else
		{
			bytesToBeRead = (int)Constants.BLOCK_SIZE;			
		}
		
		char[] newCharArray = new char[bytesToBeRead];
		
		try {
			breader.skip(offset);
			breader.read(newCharArray, 0, bytesToBeRead);
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		try {
			breader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("The new char array is "+newCharArray.length);
		return new String(newCharArray).getBytes(StandardCharsets.UTF_8);
		
	}
	
	
	static void bindToRegistry()
	{
		
		try {
			
//			Registry registry=LocateRegistry.getRegistry(Constants.NAME_NODE_IP,Registry.REGISTRY_PORT);
//			Registry registry=LocateRegistry.getRegistry("10.2.130.36",10001);
			Registry registry=LocateRegistry.getRegistry("10.2.129.126",10002);
			IDataNode stub;
			try {
				stub = (IDataNode) registry.lookup(Constants.DATA_NODE_ID);
				stub.readBlock(null);
			} catch (NotBoundException e) {
				// TODO Auto-generated catch block
				System.out.println("Could not find NameNode");
				e.printStackTrace();
			}
			
			
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	

}
