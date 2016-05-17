package com.taomee.bigdata.transfer;

public class StatInfo {
    private String stid;
    private String sstid;
    private int game;
    private int platform = -1;
    private int zone = -1;
    private int server = -1;
    private long time;
    private String uid;
    private String pid = "-1";
    private String op = null;
    private String field = null;
    public String getStid() {
        return stid;
    }
    public void setStid(String stid) {
        try {
            this.stid = new String(stid.getBytes("UTF-8"), "UTF-8");
        } catch(java.io.UnsupportedEncodingException e) { }
    }
    public String getSstid() {
        return sstid;
    }
    public void setSstid(String sstid) {
        try {
            this.sstid = new String(sstid.getBytes("UTF-8"), "UTF-8");
        } catch(java.io.UnsupportedEncodingException e) { }
    }
    public int getGame() {
        return game;
    }
    public void setGame(int game) {
        this.game = game;
    }
    public int getPlatform() {
        return platform;
    }
    public void setPlatform(int platform) {
        this.platform = platform;
    }
    public int getZone() {
        return zone;
    }
    public void setZone(int zone) {
        this.zone = zone;
    }
    public int getServer() {
        return server;
    }
    public void setServer(int server) {
        this.server = server;
    }
    public long getTime() {
        return time;
    }
    public void setTime(long time) {
        this.time = time;
    }
    public String getUid() {
        return uid;
    }
    public void setUid(String uid) {
        this.uid = uid;
    }
    public String getPid() {
        return pid;
    }
    public void setPid(String pid) {
        this.pid = pid;
    }
    public String getOp() {
        return op;
    }
    public void setOp(String op) {
        this.op = op;
    }
    public String getField() {
        return field;
    }
    public void setField(String field) {
        try {
            this.field = new String(field.getBytes("UTF-8"), "UTF-8");
        } catch(java.io.UnsupportedEncodingException e) { }
    }
}
