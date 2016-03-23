package com.hdfs.namenode;

import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Vector;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hdfs.miscl.Constants;
import com.hdfs.miscl.Hdfs.AssignBlockRequest;
import com.hdfs.miscl.Hdfs.AssignBlockResponse;
import com.hdfs.miscl.Hdfs.BlockLocations;
import com.hdfs.miscl.Hdfs.BlockReportRequest;
import com.hdfs.miscl.Hdfs.BlockReportResponse;
import com.hdfs.miscl.Hdfs.CloseFileRequest;
import com.hdfs.miscl.Hdfs.CloseFileResponse;
import com.hdfs.miscl.Hdfs.DataNodeLocation;
import com.hdfs.miscl.Hdfs.OpenFileRequest;
import com.hdfs.miscl.Hdfs.OpenFileResponse;

public class NameNodeDriver implements INameNode
{

	public static HashMap<Integer,DataNodeLocation>  dataNodes;   //data node id, location
	public static HashMap<Integer,Vector<DataNodeLocation>> blockLocations;
	public static PutFile putFile ;
	public static int numBlock=0;
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		dataNodes = new HashMap<>();
		blockLocations = new HashMap<>();
		//dataNodes.put(1,"10.0.0.2");
		
		putFile = new PutFile();
		
		bindToRegistry();
		
	}

	//In case you need my computer it's password is sheshadri@1992
	
	static void bindToRegistry()
	{
		System.setProperty("java.rmi.server.hostname",Constants.NAME_NODE_IP);
		NameNodeDriver obj = new NameNodeDriver();
		try {
			
			Registry register=LocateRegistry.createRegistry(Registry.REGISTRY_PORT);
			INameNode stub = (INameNode) UnicastRemoteObject.exportObject(obj,Registry.REGISTRY_PORT);
			try {
				register.bind(Constants.NAME_NODE, stub);
			} catch (AlreadyBoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		System.out.println("Binded succesfully");
	}



	@Override
	public byte[] openFile(byte[] inp) throws RemoteException {
		// TODO Auto-generated method stub
		
		System.out.println("Open file called");
		OpenFileResponse.Builder res = OpenFileResponse.newBuilder();
		
		OpenFileRequest req;
		try {
			req = OpenFileRequest.parseFrom(inp);
			
			String fileName = req.getFileName();
			boolean type = req.getForRead();
			
			if(!type)   // type = false then write i.e. put
			{
				int fileHandle = new Random().nextInt();
			    putFile.insertFileHandle(fileName, fileHandle);
			    
			    res.setHandle(fileHandle);
			    res.setStatus(Constants.STATUS_SUCCESS);
			    
			    return res.build().toByteArray();
				
				
			}else       // type = true then read i.e get
			{
				
			}
			
			
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		return null;
	}



	@Override
	public byte[] closeFile(byte[] inp) throws RemoteException {
		// TODO Auto-generated method stub
		CloseFileRequest req = null;
		CloseFileResponse.Builder res = CloseFileResponse.newBuilder();
		res.setStatus(Constants.STATUS_FAILED);
		
		try {
			req = CloseFileRequest.parseFrom(inp);
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Integer handle = req.getHandle();
		
		if(handle != null)
		{
			putFile.removeFileHandle(handle);
			res.setStatus(Constants.STATUS_SUCCESS);
		}
		
		
		return res.build().toByteArray();
	}



	@Override
	public byte[] getBlockLocations(byte[] inp) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}



	@Override
	public byte[] assignBlock(byte[] inp) throws RemoteException {
		// TODO Auto-generated method stub
		
		AssignBlockResponse.Builder res = AssignBlockResponse.newBuilder();
		res.setStatus(Constants.STATUS_FAILED);
		
		try {
			AssignBlockRequest req = AssignBlockRequest.parseFrom(inp);
			
			int handle = req.getHandle();
			int numBlock = getBlockNum();
			
			putFile.insertFileBlock(handle, numBlock);
			
			
			res.setStatus(Constants.STATUS_SUCCESS);
			
			BlockLocations.Builder blocks =  BlockLocations.newBuilder();
			
			
			int max = dataNodes.values().size();
			int [] randoms = getTwoRandoms(max); 
			
			List<Integer> keys      = new ArrayList<Integer>(dataNodes.keySet());
			Integer randomKey = keys.get( randoms[0]);
			DataNodeLocation value     = dataNodes.get(randomKey);
			
			blocks.addLocations(value);
			
			randomKey = keys.get( randoms[1]);
			value = dataNodes.get(randomKey);
			
			blocks.addLocations(value);
			blocks.setBlockNumber(numBlock);
			
			
			res.setNewBlock(blocks);
			
			return res.build().toByteArray();
			
			
			
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return res.build().toByteArray();
	}



	@Override
	public byte[] list(byte[] inp) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}



	@Override
	public byte[] blockReport(byte[] inp) throws RemoteException {
		// TODO Auto-generated method stub
		System.out.println("Block report called");
		BlockReportResponse.Builder res = BlockReportResponse.newBuilder();
		
		try {
			BlockReportRequest req = BlockReportRequest.parseFrom(inp);
			
			int id = req.getId();
			DataNodeLocation loc = req.getLocation();
			
			dataNodes.put(id,loc);
			
			for(int i=0;i<req.getBlockNumbersCount();i++)
			{
				int numBlock = req.getBlockNumbers(i);
				if(!blockLocations.containsKey(numBlock))
				{
					Vector<DataNodeLocation> arrLoc = new Vector<>();
					arrLoc.add(loc);
					
				}else
				{
					Vector<DataNodeLocation> tmpLoc = blockLocations.get(numBlock);
					
					if(tmpLoc.size()!=2)
					{
						if(!tmpLoc.get(0).equals(loc))
						{
							tmpLoc.add(loc);
						}
					}
				}
				
			}
			
			
			
			res.addStatus(Constants.STATUS_SUCCESS);
			
			
			
			
			
			
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return res.build().toByteArray();
	}



	@Override
	public byte[] heartBeat(byte[] inp) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}
	
	
	
	public static int getRandom(int max)
	{
		int random;
		Random rand =new Random();
		int min=0;
		random = rand.nextInt((max - min) + 1) + min;
		
		return random;
	}
	
	
	public static synchronized int getBlockNum()
	{
		return ++numBlock;
	}
	


	private int[] getTwoRandoms(int max) {
		// TODO Auto-generated method stub
		int [] randoms = new int[2];
		Random rand =new Random();
		int min=0;
		
		int random = rand.nextInt((max - min)) + min;
		int random2;
		
		randoms[0]=random;
		
		do
		{
			random2 = rand.nextInt((max - min)) + min;
		}
	    while(random==random2) ;
		
		randoms[1] = random2;
		
		return randoms;
	}
}
