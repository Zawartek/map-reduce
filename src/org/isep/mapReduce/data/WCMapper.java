package org.isep.mapReduce.data;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class WCMapper implements Function<String, List<DataPair<String, Integer>>> {

    // MUST RETURN List of DATAPAIR of word,count
    @Override
    public List<DataPair<String, Integer>> apply(String s) {
        String [] d = s.toUpperCase().split("\\W");
        List<DataPair<String,Integer>> res = new ArrayList<>();
        for(String w:d) res.add(new DataPair<>(w.toUpperCase(),1));
        return res;
    }
}
