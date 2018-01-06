package org.isep.mapReduce.server;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    protected MapReduceServer() throws RemoteException {
        super();
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
    public List<DataPair<String,Integer>> doMap(List<String> input) {
        return input.parallelStream()
                .flatMap(d -> mapper.apply(d).stream())
                .collect(Collectors.toList());
    }

    @Override
    public Map<String,List<Integer>> doShuffle(List<DataPair<String, Integer>> r) {
        Map<String, List<Integer>> m = new HashMap<>();

        for(DataPair<String, Integer> p: r) {
            List<Integer> l = m.getOrDefault(p.getKey(), new ArrayList<>());
            l.add(p.getValue());
            m.put(p.getKey(), l);
        }

        return m;
    }

    @Override
    public List<DataPair<String,Integer>> doReduce(Integer identity, Map<String, List<Integer>> data) {
        return data.entrySet().parallelStream()
                .map(e -> {
                	Integer result =identity;
                        for(Integer i: e.getValue()) {
                            result = reducer.apply(result,i);
                        }
                     return new DataPair<>(e.getKey(), result);
                        }).collect(Collectors.toList());

    }

	@Override
	public void setFileList(List<String> flist) {
		
	}
}
