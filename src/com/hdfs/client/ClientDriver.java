package com.hdfs.client;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import com.hdfs.miscl.Constants;
import com.hdfs.namenode.INameNode;

public class ClientDriver {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		bindToRegistry();
	}
	
	
	
	static void bindToRegistry()
	{
		
		try {
			
			Registry registry=LocateRegistry.getRegistry(Constants.NAME_NODE_IP,Registry.REGISTRY_PORT);
			INameNode stub;
			try {
				stub = (INameNode) registry.lookup("NameNode");
				stub.openFile(null);
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
