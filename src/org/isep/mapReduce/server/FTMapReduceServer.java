package org.isep.mapReduce.server;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.isep.mapReduce.FTMapReduce;
import org.isep.mapReduce.MapReduce;
import org.isep.mapReduce.data.DataPair;



public class FTMapReduceServer extends UnicastRemoteObject implements FTMapReduce {

    /**
	 * 
	 */
	private static final long serialVersionUID = -1144547741706116574L;
	
	public static int THREAD_POOL_SIZE = 2;
    public static int PING_FREQ = 1000;



    private boolean stop =false;




    private final Object coordinatorLock = new Object();
    private String coordinator, serverName;

    private boolean isLeader = false;
    private final ConcurrentSkipListMap<String, FTMapReduce> replicas = new ConcurrentSkipListMap<>();
    private final ExecutorService pool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);


    private final MapReduce delegate = new MapReduceServer();



    /**
     * Registration of neighbours from the leader
     * Ensures fault detection and change of leadership
     */
    private final Thread pingThread = new Thread(() -> {

        while(!stop) {
            try {
                Thread.sleep(PING_FREQ);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            String cName = null;
            synchronized (coordinatorLock) {
                cName = new String(coordinator);
            }

            FTMapReduce master = replicas.get(cName);
            if(master!=null && !isLeader) {
                List<String> repList = null;
                try {
                    System.out.println(serverName);
                    master.registerReplica(serverName,this);
                    repList = master.getNeighbors();
                } catch (RemoteException e) {
                    System.out.println("Leader Ping failed = " + e.getMessage());
                    changeLeader(cName);
                }
                updateReplicas(repList);
            }


        }
    });

    private  void connectToReplica(String address) {
        String [] parseAddr = address.split(":");
        String host = parseAddr[0];
        int port = Integer.parseInt(parseAddr[1]);
        try {
            Registry remoteTmp = LocateRegistry.getRegistry(host,port);
            FTMapReduce nvReplica = (FTMapReduce) remoteTmp.lookup(FTMapReduce.LOOKUP_NAME);
            replicas.put(address, nvReplica);
            nvReplica.registerReplica(serverName,this);
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (NotBoundException e) {
            e.printStackTrace();
        }

    }
    private void updateReplicas(List<String> repList) {
        if(repList !=null) {
            for (String nb : repList) {
                if(!replicas.containsKey(nb)) {
                    connectToReplica(nb);
                }

            }
        }
    }


    private void changeLeader(String master) {
        replicas.remove(master);
        String newLeader = replicas.firstKey();
        System.out.println(replicas);
        synchronized (coordinatorLock) {
            coordinator = newLeader;
            if(coordinator.equals(serverName))
                isLeader = true;
        }

        System.out.println("New leader is " + newLeader);
    }


    public FTMapReduceServer(String serverName, String coordinator, FTMapReduce masterServer) throws RemoteException {
        super();
        this.coordinator = coordinator;
        this.serverName = serverName;
        if(coordinator.equals(serverName)) {
            isLeader = true;
        }

        replicas.put(serverName,this);
        if(masterServer!=null) {
            replicas.put(coordinator,masterServer);
        }
    }

    /**
     * Return this node's leader name
     * @return
     * @throws RemoteException
     */
    @Override
    public String getLeader() throws RemoteException {
        String def = null;
        synchronized (coordinatorLock) {
            def = new String(coordinator);
        }
        return def;
    }

    /*
    @Override
    public String getMessage() throws RemoteException {
        return delegate.getMessage();
    }
	*/

    private void replicateMapping(List<String> datas) {
        List<Future> ftList = new ArrayList<>();
        for(Map.Entry<String, FTMapReduce> replica : replicas.entrySet()) {

            if(!serverName.equals(replica.getKey())) {
                Future f = pool.submit(() -> {
                    try {
						replica.getValue().doMap(datas);
					} catch (RemoteException e) {
						e.printStackTrace();
					}
                });

                ftList.add(f);
            }
        }

        // Wait for each task to end.
        for(Future f: ftList) {
            try {
                f.get(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        }
    }

    /*
    public void setMessage(String message) throws RemoteException {
        delegate.setMessage(message);
        // Replicates only if leader
        if(isLeader) {
            replicateMessage(message);

        }
    }
    */
    
    /**
     * Lists the names of known replicas if it is leader.
     * @return
     * @throws RemoteException
     */

    @Override
    public List<String> getNeighbors() throws RemoteException {
        List<String> replicaCopy = new ArrayList<>();
        replicaCopy.addAll(replicas.keySet());
        return replicaCopy;
    }


    @Override
    public void registerReplica(String server, FTMapReduce replica) throws RemoteException {
        replicas.put(server,replica);
        System.out.println("Registered " + server);
        // If it is leader, registers back to the replica
        /**if(isLeader) {
            replica.registerReplica(serverName,this);
        }*/

    }

    public void startPing() {
        this.pingThread.start();
    }
	@Override
	public Function<String, List<DataPair<String, Integer>>> getMapper() throws RemoteException {
		return delegate.getMapper();
	}
	@Override
	public BiFunction<Integer, Integer, Integer> getReducer() throws RemoteException {
		return delegate.getReducer();
	}
	@Override
	public List<DataPair<String, Integer>> doMap(List<String> input) throws RemoteException {
		return delegate.doMap(input);
	}
	@Override
	public Map<String, List<Integer>> doShuffle(List<DataPair<String, Integer>> r) throws RemoteException {
		return delegate.doShuffle(r);
	}
	@Override
	public List<DataPair<String, Integer>> doReduce(Integer identity, Map<String, List<Integer>> data) throws RemoteException {
		return delegate.doReduce(identity, data);
	}
	@Override
	public void setFileList(List<String> flist) {
		
	}
}
