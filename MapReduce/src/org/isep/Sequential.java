package org.isep;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class Sequential {


    public static void main(String [] args ) {

        if(args.length < 1) {
            System.out.println("USAGE: java Main index.txt");
            System.exit(0);
        }


        Map<String, Integer> result = new TreeMap<>();
        List<String> flist = new ArrayList<>();

        Main.loadFile(args[0], flist);
        List<String> lines = new ArrayList<>();

        for(String path: flist) {

            Main.loadFile(path,lines);

            for(String l: lines) {
                String[] words = l.toUpperCase().split("\\W");
                for (String w : words)
                    result.put(w, result.getOrDefault(w, 0) + 1);


            }

            lines.clear();
        }

        result.entrySet().stream().forEach(e -> System.out.println(e.getKey() + ":" + e.getValue()));
    }
}
