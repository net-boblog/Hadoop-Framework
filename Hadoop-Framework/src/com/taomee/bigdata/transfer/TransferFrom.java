package com.taomee.bigdata.transfer;

public abstract class TransferFrom {
    protected abstract void conf(String[] argv);
    protected abstract StatInfo transferLine(String s);
}
