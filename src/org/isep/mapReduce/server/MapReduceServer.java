package org.isep.mapReduce.server;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.isep.mapReduce.MapReduce;
import org.isep.mapReduce.data.DataPair;
import org.isep.mapReduce.data.WCMapper;
import org.isep.mapReduce.data.WCReducer;

public class MapReduceServer extends UnicastRemoteObject implements MapReduce {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1233176434998164899L;

    private final Function<String, List<DataPair<String,Integer>>> mapper = new WCMapper();
    private final BiFunction<Integer,Integer,Integer> reducer = new WCReducer();
    private List<String> datas;
    private List<DataPair<String, Integer>> mappedData;
    private SortedMap<String,List<Integer>> shuffledData;

    protected MapReduceServer() throws RemoteException {
        super();
        datas = new ArrayList<String>();
        mappedData = new ArrayList<DataPair<String, Integer>>();
        shuffledData = new TreeMap<String,List<Integer>>();
    }

    @Override
    public Function<String, List<DataPair<String,Integer>>> getMapper() {
    	return mapper;
    }

    @Override
    public BiFunction<Integer,Integer,Integer> getReducer() {
    	return reducer;
    }

    @Override
    public void doMap() {
    	mappedData = getData().parallelStream()
                .flatMap(d -> mapper.apply(d).stream())
                .collect(Collectors.toList());
    	datas.clear();
    	System.out.println("mapped to " + mappedData.size());
    }

    @Override
    public void doShuffle() {
    	shuffledData = new TreeMap<String,List<Integer>>();

        for(DataPair<String, Integer> p: mappedData) {
            List<Integer> l = shuffledData.getOrDefault(p.getKey(), new ArrayList<>());
            l.add(p.getValue());
            shuffledData.put(p.getKey(), l);
        }
    	System.out.println("shuffle to " + shuffledData.size());
        mappedData.clear();
    }

    @Override
    public void doReduce(Integer identity) {
    	mappedData = shuffledData.entrySet().parallelStream()
                .map(e -> {
                	Integer result =identity;
                        for(Integer i: e.getValue()) {
                            result = reducer.apply(result,i);
                        }
                     return new DataPair<>(e.getKey(), result);
                        }).collect(Collectors.toList());
    	System.out.println("reduce to " + mappedData.size());
    	shuffledData.clear();
    }

	@Override
	public void setData(List<String> data) {
		datas = data;
	}
	
	@Override
	public List<String> getData() {
		return datas;
	}

	@Override
	public List<DataPair<String, Integer>> getMappedData() {
		return mappedData;
	}

	@Override
	public void setMappedData(List<DataPair<String, Integer>> mappedData) throws RemoteException {
		this.mappedData = mappedData;
	}
	

	@Override
	public SortedMap<String,List<Integer>> getShuffledData() {
		return shuffledData;
	}

	@Override
	public void setShuffledData(SortedMap<String,List<Integer>> shuffledData) throws RemoteException {
		this.shuffledData = shuffledData;
	}

	@Override
	public void clearAll() throws RemoteException {
		if (this.datas != null) {
			this.datas.clear();
		}
		if (this.shuffledData != null) {
			shuffledData.clear();
		}
		if (this.mappedData != null) {
			this.mappedData.clear();
		}
	}
}
