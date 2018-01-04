package org.isep.mapReduce.data;

import java.io.Serializable;

public class DataPair<K,V> implements Serializable{
    private K key;
    private V value;

    public DataPair(K key, V value) {
        this.key = key;
        this.value = value;
    }
    
    public K getKey() {
    	return key;
    }

    public void setKey(K key2) {
    	key = key2;
    }
    
    public V getValue() {
    	return value;
    }
    
    public void setValue(V value2) {
    	value = value2;
    }
}
