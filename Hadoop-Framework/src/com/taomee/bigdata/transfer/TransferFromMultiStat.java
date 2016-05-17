package com.taomee.bigdata.transfer;

import java.util.HashMap;
import java.lang.RuntimeException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.IOException;

public class TransferFromMultiStat extends TransferFrom {//从多区服统计读源数据
    //op类型
    private static final int T_UCOUNT   = 0;
    private static final int T_COUNT    = 1;
    private static final int T_SUM      = 2;
    private static final int T_MAX      = 3;
    private static final int T_SET      = 4;
    private static final int T_ITEMUC   = 5;
    private static final int T_ITEMCO   = 6;
    private static final int T_ITEMSUM  = 7;
    private static final int T_ITEMMAX  = 8;
    private static final int T_ITEMSET  = 9;
    private static final int T_DISTRSUM = 10;
    private static final int T_DISTRMAX = 11;
    private static final int T_DISTRSET = 12;

    //多区服的文件类型
    private static final int F_DEFAULT  = 0; //人数人次
    private static final int F_ITEM     = 1; //item人数人次(算术)
    private static final int F_ARITH    = 2; //算术
    private static final int F_DISTR    = 3; //等级分布

    private int type = -1;// 多区服文件类型
    private int game = -1;
    private String confFile = null;
    private HashMap<String, Integer> typeMap = new HashMap<String, Integer>();
    private HashMap<Long, StatInfo> statInfoMap = new HashMap<Long, StatInfo>();
    private HashMap<String, String> itemInfoMap = new HashMap<String, String>();

    public TransferFromMultiStat() {
        typeMap.put("default", F_DEFAULT);
        typeMap.put("item", F_ITEM);
        typeMap.put("arithmatic", F_ARITH);
        typeMap.put("arith", F_ARITH);
        typeMap.put("distr", F_DISTR);
    }

    protected void conf(String[] argv) {
        for(int i=0; i<argv.length; i++) {
            if(argv[i].compareToIgnoreCase("-type") == 0) {
                Integer type;
                if((type = typeMap.get(argv[++i])) != null) {
                    this.type = type;
                } else {
                    throw new RuntimeException("type defined error");
                }
            } else if(argv[i].compareToIgnoreCase("-conf") == 0) {
                confFile = argv[++i];
                loadConfFromFile(confFile);
            } else if(argv[i].compareToIgnoreCase("-game") == 0) {
                game = Integer.valueOf(argv[++i]);
            }
        }
        if(confFile == null) {
            throw new RuntimeException("conf not defined");
        }
        if(game == -1) {
            throw new RuntimeException("game not defined");
        }
        if(type == -1) {
            throw new RuntimeException("type not defined");
        }

        loadItemInfo();
    }

    protected StatInfo transferLine(String s) {
        switch(type) {
            case F_DEFAULT: 
                return defaultLine(s);
            case F_ITEM   :
                return itemLine(s);
            case F_ARITH  :
                return arithLine(s);
            case F_DISTR  :
                return distrLine(s);
        }
        return null;
    }

    private StatInfo defaultLine(String s) {
        String[] items = s.split("\t");
        Long msgId = Long.valueOf(items[4]);
        StatInfo confInfo = statInfoMap.get(msgId);
        if(confInfo != null) {
            StatInfo statInfo = new StatInfo();
            statInfo.setTime(Long.valueOf(items[0]));
            statInfo.setUid(items[3]);
            statInfo.setGame(game);
            statInfo.setZone(Integer.valueOf(items[1]));
            statInfo.setServer(Integer.valueOf(items[2]));
            statInfo.setStid(confInfo.getStid());
            statInfo.setSstid(confInfo.getSstid());
            if(confInfo.getField() != null) {
                statInfo.setField("item=" + confInfo.getField());
                statInfo.setOp("item:item");
            }
            return statInfo;
        } else {
            return null;
        }
    }

    private StatInfo itemLine(String s) {
        String[] items = s.split("\t");
        Long msgId = Long.valueOf(items[4]);
        StatInfo confInfo = statInfoMap.get(msgId);
        if(confInfo != null) {
            StatInfo statInfo = new StatInfo();
            String[] ss = items[5].split("#");
            String item = itemInfoMap.get(msgId + ":" + ss[0]);
            if(item == null)    return null;
            if(confInfo.getOp() != null &&
                    confInfo.getOp().compareToIgnoreCase("task") == 0) {
                statInfo.setTime(Long.valueOf(items[0]));
                statInfo.setUid(items[3]);
                statInfo.setGame(game);
                statInfo.setZone(Integer.valueOf(items[1]));
                statInfo.setServer(Integer.valueOf(items[2]));
                statInfo.setStid(confInfo.getStid());
                statInfo.setSstid(item);
                return statInfo;
            }
            String itemname = confInfo.getField() == null ? "item" : confInfo.getField();
            if(ss.length == 2) {
                statInfo.setField(itemname + "=" + item + "\tvalue=" + ss[1]);
            } else {
                statInfo.setField(itemname + "=" + item);
            }
            statInfo.setTime(Long.valueOf(items[0]));
            statInfo.setUid(items[3]);
            statInfo.setGame(game);
            statInfo.setZone(Integer.valueOf(items[1]));
            statInfo.setServer(Integer.valueOf(items[2]));
            statInfo.setStid(confInfo.getStid());
            statInfo.setSstid(confInfo.getSstid());
            if(confInfo.getOp() != null){
                statInfo.setOp("item:" + itemname + "|" + confInfo.getOp() + ":" + itemname + ",value");
            } else {
                statInfo.setOp("item:" + itemname);
            }
            return statInfo;
        } else {
            return null;
        }
    }

    private StatInfo arithLine(String s) {
        String[] items = s.split("\t");
        Long msgId = Long.valueOf(items[4]);
        StatInfo confInfo = statInfoMap.get(msgId);
        if(confInfo != null) {
            StatInfo statInfo = new StatInfo();
            statInfo.setTime(Long.valueOf(items[0]));
            statInfo.setUid(items[3]);
            statInfo.setGame(game);
            statInfo.setZone(Integer.valueOf(items[1]));
            statInfo.setServer(Integer.valueOf(items[2]));
            statInfo.setStid(confInfo.getStid());
            statInfo.setSstid(confInfo.getSstid());
            statInfo.setOp(confInfo.getOp() + ":" + confInfo.getField());
            statInfo.setField(confInfo.getField() + "=" + items[5]);
            return statInfo;
        } else {
            return null;
        }
    }

    private StatInfo distrLine(String s) {
        String[] items = s.split("\t");
        Long msgId = Long.valueOf(items[4]);
        StatInfo confInfo = statInfoMap.get(msgId);
        if(confInfo != null) {
            StatInfo statInfo = new StatInfo();
            statInfo.setTime(Long.valueOf(items[0]));
            statInfo.setUid(items[3]);
            statInfo.setGame(game);
            statInfo.setZone(Integer.valueOf(items[1]));
            statInfo.setServer(Integer.valueOf(items[2]));
            statInfo.setStid(confInfo.getStid());
            statInfo.setSstid(confInfo.getSstid());
            statInfo.setOp(confInfo.getOp() + "_distr:value");
            statInfo.setField("value=" + items[5]);
            return statInfo;
        } else {
            return null;
        }
    }

    private void loadConfFromFile(String f) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(f));
            String line;
            while((line = reader.readLine()) != null) {
                String[] items = line.split("[\t ]");
                Long id = Long.valueOf(items[0]);
                StatInfo statInfo = new StatInfo();
                for(int i=1; i<items.length; i++) {
                    String[] ss = items[i].split("=");
                    String key = ss[0];
                    String value = ss[1];
                    if(key.compareToIgnoreCase("stid") == 0) {
                        statInfo.setStid(value);
                    } else if(key.compareToIgnoreCase("sstid") == 0) {
                        statInfo.setSstid(value);
                    } else if(key.compareToIgnoreCase("op") == 0) {
                        statInfo.setOp(value);
                    } else if(key.compareToIgnoreCase("field") == 0) {
                        statInfo.setField(value);
                    }
                }
                statInfoMap.put(id, statInfo);
            }
            reader.close();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e.getMessage());
        } catch (IOException e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
        }
    }

    private void loadItemInfo() {
        try {
            BufferedReader reader = new BufferedReader(new FileReader("item.log"));
            String line;
            while((line = reader.readLine()) != null) {
                String[] items = line.split(" ");
                try {
                    itemInfoMap.put(items[0], new String(items[1].getBytes("UTF-8"), "UTF-8"));
                } catch(java.io.UnsupportedEncodingException e) { }
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e.getMessage());
        } catch (IOException e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
        }
    }
}
