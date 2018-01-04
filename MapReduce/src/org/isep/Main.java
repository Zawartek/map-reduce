package org.isep;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Main {

    private static int BATCH_SIZE = 250;



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


    public static <K,V,D> List<DataPair<K,V>> map(Function<D,List<DataPair<K,V>>> m, List<D> input) {
        return input.parallelStream()
                .flatMap(d -> m.apply(d).stream())
                .collect(Collectors.toList());
    }


    public static <K,V> Map<K,List<V>> shuffle(List<DataPair<K, V>> r) {
        Map<K, List<V>> m = new HashMap<>();

        for(DataPair<K, V> p: r) {
            List<V> l = m.getOrDefault(p.key, new ArrayList<>());
            l.add(p.value);
            m.put(p.key, l);
        }

        return m;
    }


    public static <k,outv,inv>
                List<DataPair<k,outv>>
                reduce(BiFunction<outv,inv,outv> reduc, outv identity, Map<k,List<inv>> data) {

        return data.entrySet().parallelStream()
                .map(e -> {
                        outv result =identity;
                        for(inv i: e.getValue()) {
                            result = reduc.apply(result,i);
                        }
                     return new DataPair<>(e.getKey(), result);
                        }).collect(Collectors.toList());

    }


    /** CORRECTION */
    static final Function<String, List<DataPair<String,Integer>>> mapper = new WCMapper();
            /*(String s) -> {
          String [] d = s.toUpperCase().split("\\W");
          List<DataPair<String,Integer>> res = new ArrayList<>();
          for(String w:d) res.add(new DataPair<>(w.toUpperCase(),1));
          return res;
        };
    */
    static final BiFunction<Integer,Integer,Integer> reducer = new WCReducer();//(Integer a, Integer b) -> (a+b);
    /* FIN CORRECTion */
    public static void main(String[] args) {

        if(args.length < 1) {
            System.out.println("USAGE: java Main index.txt");
            System.exit(0);
        }

        List<String> flist = new ArrayList<>();
        loadFile(args[0], flist);


        List<String> data = new ArrayList<>();
        List<DataPair<String,Integer>> resultMain = new ArrayList<>();
        int count = 0;
        // Loading data
        for(String p: flist) {
            loadFile(p,data);
            if (++count % BATCH_SIZE == 0 || flist.size() == count) {

                //System.out.println("MAP PHASE");
                List<DataPair<String, Integer>> r = map(mapper, data);
                // System.out.println("Heap size is: " + Runtime.getRuntime().totalMemory() + " ON " + Runtime.getRuntime().maxMemory());

                //System.out.println("SHUFFLE PHASE");
                Map<String, List<Integer>> shuffled = shuffle(r);
                //System.out.println("Heap size is: " + Runtime.getRuntime().totalMemory() + " ON " + Runtime.getRuntime().maxMemory());

                //System.out.println("REDUCE PHASE");
                List<DataPair<String, Integer>> result = reduce(reducer, 0, shuffled);
                //System.out.println("Heap size is: " + Runtime.getRuntime().totalMemory() + " ON " + Runtime.getRuntime().maxMemory());


                //System.out.println("Reduce SHUFFLE PHASE");
                result.addAll(resultMain);
                Map<String, List<Integer>> shuffled2 = shuffle(result);
                //System.out.println("TREE REDUCE PHASE");
                resultMain = reduce(reducer, 0, shuffled2);
                //System.out.println("Heap size is: " + Runtime.getRuntime().totalMemory() + " ON " + Runtime.getRuntime().maxMemory());
                data.clear();
                //System.out.println("Processed:" + count + " on " + flist.size());
            }

        }


        //printing results
        resultMain.forEach(d -> System.out.println(d.key +";" + d.value));



        }



}
