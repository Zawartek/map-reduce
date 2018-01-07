package org.isep.mapReduce.server;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
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
                    //System.out.println(serverName);
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

    private void replicateMapping() {
        List<Future> ftList = new ArrayList<>();
        for(Map.Entry<String, FTMapReduce> replica : replicas.entrySet()) {

            if(!serverName.equals(replica.getKey())) {
                Future f = pool.submit(() -> {
                    try {
						replica.getValue().doMap();
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
                f.get(100, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        }
    }

    private void replicateReduce(Integer identity) {
        List<Future> ftList = new ArrayList<>();
        for(Map.Entry<String, FTMapReduce> replica : replicas.entrySet()) {

            if(!serverName.equals(replica.getKey())) {
                Future f = pool.submit(() -> {
                    try {
						replica.getValue().doReduce(identity);
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
                f.get(100, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        }
    }
    
    
    private void replicateData(List<String> datas) {
        List<String> replicateData = null;
        int cpt = 0, start=0, end=0;
        for(Map.Entry<String, FTMapReduce> replica : replicas.entrySet()) {
        	
            if(!serverName.equals(replica.getKey())) {
	        	start = cpt*(datas.size()/(replicas.size()-1)+1);
	        	end = start + (datas.size()/(replicas.size()-1)+1);
                cpt++;
	        	if (end>datas.size() || cpt==(replicas.size()-1)) {
	        		end = datas.size();
	        	}
                try {
                	replica.getValue().setData(new ArrayList<>(datas.subList(start, end)));
				} catch (RemoteException e) {
					e.printStackTrace();
				}
            }
        }
    }
    
    private void replicateMappedData() throws RemoteException {
        List<String> replicateData = null;
        int cpt = 0, start=0, end=0;
        List<Future> ftList = new ArrayList<>();
        for(Map.Entry<String, FTMapReduce> replica : replicas.entrySet()) {

            if(!serverName.equals(replica.getKey())) {
	        	start = cpt*(getMappedData().size()/(replicas.size()-1)+1);
	        	end = start + (getMappedData().size()/(replicas.size()-1)+1);
                cpt++;
	        	if (end>getMappedData().size() || cpt==(replicas.size()-1)) {
	        		end = getMappedData().size();
	        	}
            	Future f = doReplicateMappedData(replica, start, end);

                ftList.add(f);
            }
        }

        // Wait for each task to end.
        for(Future f: ftList) {
            try {
                f.get(100, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        }
    }
    
    private Future doReplicateMappedData(Map.Entry<String, FTMapReduce> replica,
    									int start, int end) {
    	Future f = pool.submit(() -> {
            try {
            	replica.getValue().setMappedData(new ArrayList<>(getMappedData().subList(start, end)));
			} catch (RemoteException e) {
				e.printStackTrace();
			}
        });
    	return f;
    }

    private void replicateShuffledData() throws RemoteException {
        List<String> replicateData = null;
        String start="", end="";
        Iterator<String> keySet = getShuffledData().keySet().iterator();
        List<Future> ftList = new ArrayList<>();
    	System.out.println("ShuffledDatas from " + getShuffledData().firstKey() + " to " + getShuffledData().lastKey());
    	System.out.println("Shuffled Datas: " + getShuffledData().size()
    			+ " Slaves server: " + (replicas.size()-1)
    			+ " Data count by slave " + (getShuffledData().size()/(replicas.size()-1)));
        for(Map.Entry<String, FTMapReduce> replica : replicas.entrySet()) {

            if(!serverName.equals(replica.getKey())) {
	        	start = keySet.next();
	        	end = start;
	        	for(int cpt=0;cpt<=(getShuffledData().size()/(replicas.size()-1)+1) && keySet.hasNext();++cpt) {
	        		end = keySet.next();
	        	}
	        	System.out.println(start + " - " + end);
            	Future f = doReplicateShuffledData(replica, start, end);

                ftList.add(f);
            }
        }

        // Wait for each task to end.
        for(Future f: ftList) {
            try {
                f.get(100, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        }
    }
    
    private Future doReplicateShuffledData(Map.Entry<String, FTMapReduce> replica,
    									String start, String end) {
    	Future f = pool.submit(() -> {
            try {
            	replica.getValue().setShuffledData(getShuffledData().subMap(start, end));
			} catch (RemoteException e) {
				e.printStackTrace();
			}
        });
    	return f;
    }
    
    private void replicateGetMappedData() throws RemoteException {
        List<Future> ftList = new ArrayList<>();
        for(Map.Entry<String, FTMapReduce> replica : replicas.entrySet()) {

            if(!serverName.equals(replica.getKey())) {
                Future f = pool.submit(() -> {
                	List<DataPair<String,Integer>> resultTemp;
                    try {
                    	resultTemp = replica.getValue().getMappedData();
                    	resultTemp.addAll(delegate.getMappedData());
                    	delegate.setMappedData(resultTemp);
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
                f.get(100, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        }
    }
    
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
        //System.out.println("Registered " + server);
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
	public void doMap() throws RemoteException {
        if(isLeader) {
            replicateMapping();
        }
        else {
        	delegate.doMap();
        }
	}
	@Override
	public void doShuffle() throws RemoteException {
		if (isLeader) {
			delegate.setMappedData(getAllMappedData());
			delegate.doShuffle();
			replicateShuffledData();
		}
		else {
			delegate.doShuffle();
		}
	}
	@Override
	public void doReduce(Integer identity) throws RemoteException {
        if(isLeader) {
            replicateReduce(identity);
        }
        else {
        	delegate.doReduce(identity);
        }
	}
	@Override
	public void setData(List<String> data) throws RemoteException {
        if(isLeader) {
            replicateData(data);
        }
        else {
        	delegate.setData(data);
        }
	}
	@Override
	public List<String> getData() throws RemoteException {
		return delegate.getData();
	}
	@Override
	public List<DataPair<String, Integer>> getMappedData() throws RemoteException {
		return delegate.getMappedData();
	}
	@Override
	public void setMappedData(List<DataPair<String, Integer>> mappedData) throws RemoteException {
        if(isLeader) {
        	delegate.setMappedData(mappedData);
            replicateMappedData();
        }
        else {
        	delegate.setMappedData(mappedData);
        }
	}

	@Override
	public List<DataPair<String, Integer>> getAllMappedData() throws RemoteException {
        if(isLeader) {
            replicateGetMappedData();
        }
        return getMappedData();
	}
	@Override
	public void clearAll() throws RemoteException {
		delegate.clearAll();
	}
	@Override
	public void setShuffledData(SortedMap<String, List<Integer>> shuffledData) throws RemoteException {
		delegate.setShuffledData(shuffledData);
	}
	@Override
	public SortedMap<String, List<Integer>> getShuffledData() throws RemoteException {
		return delegate.getShuffledData();
	}
}
