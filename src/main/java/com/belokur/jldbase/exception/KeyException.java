package com.belokur.jldbase.exception;

public class KeyException extends RuntimeException {
    public KeyException(String key) {
        super("Key: " + key + " was not found");
    }
}
