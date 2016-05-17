package com.taomee.bigdata.util;
 
import sun.misc.Signal;

public interface SignalCallback {
    public void signalCallback(Signal sn, TaomeeThread thread);
}
