package com.taomee.bigdata.util;

import sun.misc.Signal;
import sun.misc.SignalHandler;
import java.util.HashMap;
import java.util.Iterator;

class defaultTermCallback implements SignalCallback {
    public void signalCallback(Signal sn, TaomeeThread thread) {
        thread.stop();
    }
}

//@SuppressWarnings("all")
public abstract class TaomeeThread implements SignalHandler {
    private boolean run = false;
    private long sleepTime = 1;
    private HashMap<String, SignalCallback> callBack = new HashMap<String, SignalCallback> ();

    public static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (Exception e) { }
    }

    public TaomeeThread() {
        register("TERM", new defaultTermCallback());
    }

    public void setSleepTime(long second) {
        sleepTime = second >= 1 ? second : 1;
    }

    final public long getSleepTime() {
        return sleepTime;
    }

    final public boolean isRunning() {
        return run;
    }

    final protected void stop() {
        run = false;
    }

    final protected void register(String signalName, SignalCallback cb) {
        callBack.put(signalName, cb);
    }

    final protected SignalCallback unregister(String signalName) {
        return callBack.remove(signalName);
    }

    protected abstract void exit();

    protected abstract void process(String[] args);
 
    @Override
    public void handle(Signal signalName) {
        SignalCallback cb = callBack.get(signalName.getName());
        if(cb != null)
            cb.signalCallback(signalName, this);
    }

    final public void run(String[] args) {
        Iterator<String> it = callBack.keySet().iterator();
        while(it.hasNext()) {
            Signal.handle(new Signal(it.next().toUpperCase()), this);
        }

        run = true;
        for (;run;) {
            process(args);
            if(!run)    break;
            for(int i=0; i<sleepTime && run; i++) {
                sleep(1000);
            }
        }
        exit();
    }

}
