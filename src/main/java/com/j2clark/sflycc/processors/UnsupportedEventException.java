package com.j2clark.sflycc.processors;

public class UnsupportedEventException extends Exception {

    // definitely need to add exception detail
    // transactionId, raw event data, why exactly is it unsupported

    public UnsupportedEventException(String msg) {
        super(msg);
    }

    public UnsupportedEventException(Throwable t) {
        super(t);
    }



}
