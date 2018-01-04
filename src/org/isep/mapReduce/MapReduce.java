package org.isep.mapReduce;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.isep.mapReduce.data.DataPair;

public interface MapReduce extends Remote {
	
	public Function<String, List<DataPair<String,Integer>>> getMapper() throws RemoteException;
	public BiFunction<Integer,Integer,Integer> getReducer() throws RemoteException;
	public List<DataPair<String,Integer>> doMap(List<String> input) throws RemoteException;
	public Map<String,List<Integer>> doShuffle(List<DataPair<String, Integer>> r) throws RemoteException;
	 public List<DataPair<String,Integer>> doReduce(Integer identity, Map<String, List<Integer>> data) throws RemoteException;
}
