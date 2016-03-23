package com.hdfs.datanode;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;



public class DataNodeDriver implements IDataNode {

	/**Interface methods start here **/
	public byte[] readBlock(byte[] inp) throws RemoteException {
		// TODO Auto-generated method stub
		
		return null;
	}


	public byte[] writeBlock(byte[] inp) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	/**Interface methods end here **/
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		System.out.println("Datanode "+ args[0]);
		/**Need an argument from command line to uniquely identify the data Node **/
		DataNodeDriver dataDriverObj = new DataNodeDriver();
		
		Registry register = null;

		//Registering an object in java RMI environment
		try
		{
			IDataNode dataStub = (IDataNode) UnicastRemoteObject.exportObject(dataDriverObj);
			register = LocateRegistry.createRegistry(8000);
			register.rebind("Datanode1",dataStub);
		}
		catch(RemoteException e)
		{
			System.out.println("Remote Exception Caught DataNodeDriverClas  method:main");
		}
		
	}
	
}
