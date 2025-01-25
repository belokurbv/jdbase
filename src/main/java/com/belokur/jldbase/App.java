package com.belokur.jldbase;

import com.belokur.jldbase.v1.LogStorageV2;

public class App {
    public static void main(String[] args) {
        var kv = new LogStorageV2("simple.out");
        kv.set("crap", "piss");
        kv.set("cum", "shot");
        System.out.println(kv.get("cum"));
    }
}
