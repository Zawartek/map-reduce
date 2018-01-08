package org.isep.mapReduce;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;

import org.isep.mapReduce.data.*;
import org.isep.mapReduce.server.FTMapReduceServer;

public class ClientMain {

    private static int BATCH_SIZE = 250;
    static private Registry reg = null;
    static private FTMapReduce currentServer = null;
    static private String serverID = null;
	static private List<String> serverReplicas = null;



    public static void loadFile(String path, List<String> data) {
        System.out.println("Loading: " + path);

        if(path.length() <= 1) return;
        try {
            List<String> lines  = Files.readAllLines(new File(path).toPath());
            data.addAll(lines);
        } catch (InvalidPathException e) {
        	System.out.println("Error with the path of the file: " + path);
        } catch (Exception e) {
        	System.out.println("Error with file: " + path);
        }
    }



    public static void main(String[] args) throws RemoteException {
        if(args.length != 3) {
            System.out.println("USAGE: java Main index.txt");
            System.exit(0);
        }

        String address = args[0];
        int port = Integer.parseInt(args[1]);
        
        serverID = address+":"+port;

        connection(address, port);

        List<String> flist = new ArrayList<>();
        System.out.println("Loading List files");
        loadFile(args[2], flist);

        List<String> data = new ArrayList<>();
        Date before, after;
        before = new Date(System.currentTimeMillis());
        List<DataPair<String,Integer>> resultMain = new ArrayList<>();
        List<DataPair<String,Integer>> resultTemp;
        int count = 0;
        System.out.println("Loading files");
        for(String p: flist) {
            loadFile(p,data);
            if (++count % BATCH_SIZE == 0 || flist.size() == count) {
            	currentServer.setData(data);
                //System.out.println("MAP PHASE");
                currentServer.doMap();
                // System.out.println("Heap size is: " + Runtime.getRuntime().totalMemory() + " ON " + Runtime.getRuntime().maxMemory());

                //System.out.println("SHUFFLE PHASE");
                currentServer.doShuffle();
                //System.out.println("Heap size is: " + Runtime.getRuntime().totalMemory() + " ON " + Runtime.getRuntime().maxMemory());

                //System.out.println("REDUCE PHASE");
                currentServer.doReduce(0);
                //System.out.println("Heap size is: " + Runtime.getRuntime().totalMemory() + " ON " + Runtime.getRuntime().maxMemory());
                resultTemp = currentServer.getAllMappedData();
                resultTemp.addAll(resultMain);
                currentServer.setMappedData(resultTemp);
                currentServer.doShuffle();
                //System.out.println("TREE REDUCE PHASE");
                currentServer.doReduce(0);
                //System.out.println("Heap size is: " + Runtime.getRuntime().totalMemory() + " ON " + Runtime.getRuntime().maxMemory());
                
                resultMain = currentServer.getAllMappedData();
                data.clear();
                //System.out.println("Processed:" + count + " on " + flist.size());
            }
        }
        //printing results
        after = new Date(System.currentTimeMillis());
        resultMain.forEach(d -> System.out.println(d.getKey() +";" + d.getValue()));
        System.out.println("print " + resultMain.size() + " results");
        System.out.println("Before: "+before + " - after: " + after);
    }

    public static void connection(String address, int port) {
        try {
            reg = LocateRegistry.getRegistry(address,port);
        } catch (RemoteException e) {
            e.printStackTrace();
        }

        try {
            currentServer = (FTMapReduce) reg.lookup(FTMapReduce.LOOKUP_NAME);
            serverReplicas = (currentServer.getNeighbors());
            System.out.println(serverReplicas);
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (NotBoundException e) {
			e.printStackTrace();
		}
    }

}
