package org.isep;

import java.util.function.BiFunction;

public class WCReducer implements BiFunction<Integer, Integer, Integer> {

    // SHould sum its two parameters
    @Override
    public Integer apply(Integer integer, Integer integer2) {
        return integer+integer2;
    }
}
