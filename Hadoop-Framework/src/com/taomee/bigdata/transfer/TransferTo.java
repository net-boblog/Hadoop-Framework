package com.taomee.bigdata.transfer;

public abstract class TransferTo {
    protected abstract void conf(String[] argv);
    protected abstract String transferStat(StatInfo stat);
}
