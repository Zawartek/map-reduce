package org.isep.mapReduce;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.SortedMap;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.isep.mapReduce.data.DataPair;

public interface MapReduce extends Remote {
	
	public Function<String, List<DataPair<String,Integer>>> getMapper() throws RemoteException;
	public BiFunction<Integer,Integer,Integer> getReducer() throws RemoteException;
	public void doMap() throws RemoteException;
	public void doShuffle() throws RemoteException;
	public void doReduce(Integer identity) throws RemoteException;
	public void setData(List<String> data) throws RemoteException;
	public List<String> getData() throws RemoteException;
	List<DataPair<String, Integer>> getMappedData() throws RemoteException;
	public void setMappedData(List<DataPair<String, Integer>> mappedData) throws RemoteException;
	public void clearAll() throws RemoteException;
	void setShuffledData(SortedMap<String, List<Integer>> shuffledData) throws RemoteException;
	SortedMap<String, List<Integer>> getShuffledData() throws RemoteException;
}
