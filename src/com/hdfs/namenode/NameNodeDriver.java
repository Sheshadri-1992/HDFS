package com.hdfs.namenode;

import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import com.hdfs.miscl.Constants;

public class NameNodeDriver implements INameNode
{

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		bindToRegistry();
		
	}

	
	
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
		
		return null;
	}



	@Override
	public byte[] closeFile(byte[] inp) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}



	@Override
	public byte[] getBlockLocations(byte[] inp) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}



	@Override
	public byte[] assignBlock(byte[] inp) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
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
		
		return null;
	}



	@Override
	public byte[] heartBeat(byte[] inp) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}
}
