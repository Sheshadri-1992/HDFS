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

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.hdfs.datanode.IDataNode;
import com.hdfs.miscl.Constants;
import com.hdfs.miscl.Hdfs.AssignBlockRequest;
import com.hdfs.miscl.Hdfs.AssignBlockResponse;
import com.hdfs.miscl.Hdfs.BlockLocations;
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
					AssignBlockResponse assignResponseObj ;
					BlockLocations blkLocation ;
					List<DataNodeLocation> dataNodeLocations;
					DataNodeLocation dataNode;
					WriteBlockRequest.Builder writeBlockObj = WriteBlockRequest.newBuilder();
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
						
						writeBlockObj.setBlockInfo(blkLocation);
//						writeBlockObj.setData(0, ByteString.copyFrom(byteArray));
						writeBlockObj.addData(ByteString.copyFrom(byteArray));
						
						dataStub.writeBlock(writeBlockObj.build().toByteArray());
//						byteArray = null;

						
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
//		int bytesLeft = (int)Constants.BLOCK_SIZE; // Or whatever
//		try
//		{
//		  
//		  
//		    while (bytesLeft > 0) {
//		      int read = fis.read(byteArray, 0, Math.min(bytesLeft, byteArray.length));
//		      if (read == -1) {
////		        throw new EOFException("Unexpected end of data");
//		      }
//		  
//		      bytesLeft -= read;
//		    }
//		  
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			System.out.println("Its here");
//			e.printStackTrace();
//		} 
		
		
		int bytesToRead;
		if((offset+(int)Constants.BLOCK_SIZE)>FILESIZE)
		{
			bytesToRead = (int)(FILESIZE - offset);
		}
		else
		{
			bytesToRead = (int)Constants.BLOCK_SIZE;
		}
		
		byte[] byteArray = new byte[bytesToRead];
		System.out.println("Bytes to read are "+bytesToRead+" Size of byte Array is "+byteArray.length);
		
		try {
			fis.read(byteArray,0,bytesToRead);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return byteArray;
	}
	
	
	static void bindToRegistry()
	{
		
		try {
			
//			Registry registry=LocateRegistry.getRegistry(Constants.NAME_NODE_IP,Registry.REGISTRY_PORT);
			Registry registry=LocateRegistry.getRegistry("10.2.130.36",10001);
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
