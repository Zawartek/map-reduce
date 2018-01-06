package org.isep.mapReduce;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
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
        } catch (IOException e) {
            e.printStackTrace();
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
        List<DataPair<String,Integer>> resultMain = new ArrayList<>();
        int count = 0;
        // Loading data
        currentServer.setFileList(flist);
        System.out.println("Loading files");
        for(String p: flist) {
            loadFile(p,data);
            if (++count % BATCH_SIZE == 0 || flist.size() == count) {
                //System.out.println("MAP PHASE");
                List<DataPair<String, Integer>> r = currentServer.doMap(data);
                // System.out.println("Heap size is: " + Runtime.getRuntime().totalMemory() + " ON " + Runtime.getRuntime().maxMemory());

                //System.out.println("SHUFFLE PHASE");
                Map<String, List<Integer>> shuffled = currentServer.doShuffle(r);
                //System.out.println("Heap size is: " + Runtime.getRuntime().totalMemory() + " ON " + Runtime.getRuntime().maxMemory());

                //System.out.println("REDUCE PHASE");
                List<DataPair<String, Integer>> result = currentServer.doReduce(0, shuffled);
                //System.out.println("Heap size is: " + Runtime.getRuntime().totalMemory() + " ON " + Runtime.getRuntime().maxMemory());


                //System.out.println("Reduce SHUFFLE PHASE");
                result.addAll(resultMain);
                Map<String, List<Integer>> shuffled2 = currentServer.doShuffle(result);
                //System.out.println("TREE REDUCE PHASE");
                resultMain = currentServer.doReduce(0, shuffled2);
                //System.out.println("Heap size is: " + Runtime.getRuntime().totalMemory() + " ON " + Runtime.getRuntime().maxMemory());
                data.clear();
                //System.out.println("Processed:" + count + " on " + flist.size());
            }
        }
        //printing results
        resultMain.forEach(d -> System.out.println(d.getKey() +";" + d.getValue()));
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
