package com.taomee.bigdata.lib;

import java.util.HashMap;
import java.util.Iterator;

public final class Operator
{
    public final static int EMPTY           = -1;
    public final static int UCOUNT          =  0;
    public final static int COUNT           =  1;
    public final static int SUM             =  2;
    public final static int MAX             =  3;
    public final static int SET             =  4;
    public final static int DISTR_SUM       =  5;
    public final static int DISTR_MAX       =  6;
    public final static int DISTR_SET       =  7;
    public final static int IP_DISTR        =  8;
    public final static int ITEM            =  9;
    public final static int ITEM_SUM        = 10;
    public final static int ITEM_MAX        = 12;
    public final static int ITEM_SET        = 13;
    public final static int ITEM_DISTR      = 14;
    public final static int ITEM_UCOUNT     = 15;
    public final static int ITEM_COUNT      = 16;
    public final static int HIP_COUNT       = 100;

    private final static HashMap<String, Integer> opSet = new HashMap<String, Integer>();
    private final static Operator _operator = new Operator();
    private static String[] codeSet;
    
    private Operator() {
        opSet.put("UCOUNT",         UCOUNT);
        opSet.put("COUNT",          COUNT);
        opSet.put("SUM",            SUM);
        opSet.put("MAX",            MAX);
        opSet.put("SET",            SET);
        opSet.put("SUM_DISTR",      DISTR_SUM);
        opSet.put("MAX_DISTR",      DISTR_MAX);
        opSet.put("SET_DISTR",      DISTR_SET);
        opSet.put("IP_DISTR",       IP_DISTR);
        opSet.put("ITEM",           ITEM);
        opSet.put("ITEM_SUM",       ITEM_SUM);
        opSet.put("ITEM_MAX",       ITEM_MAX);
        opSet.put("ITEM_SET",       ITEM_SET);
        opSet.put("ITEM_DISTR",     ITEM_DISTR);
        opSet.put("ITEM_UCOUNT",    ITEM_UCOUNT);
        opSet.put("ITEM_COUNT",     ITEM_COUNT);
        opSet.put("DISTR_SUM",      DISTR_SUM);
        opSet.put("DISTR_MAX",      DISTR_MAX);
        opSet.put("DISTR_SET",      DISTR_SET);
        codeSet = new String[opSet.size()];
        Iterator<String> it = opSet.keySet().iterator();
        while(it.hasNext()) {
            String code = it.next();
            codeSet[opSet.get(code)] = code;
        }
    }

    public static boolean isOperator(String op) {
        return _operator.opSet.containsKey(op.trim().toUpperCase());
    }

    public static Integer getOperatorCode(String op) {
        Integer ret = _operator.opSet.get(op.trim().toUpperCase());
        return ret == null ? EMPTY : ret;
    }

    public static String getOperatorCode(Integer op) {
        return (op < 0 || op >= codeSet.length) ? "EMPTY" : codeSet[op];
    }

    public static void main(String[] args) {
        System.out.println(Operator.getOperatorCode("IP_DISTR"));   //8
        System.out.println(Operator.getOperatorCode("ucount"));     //0
        System.out.println(Operator.getOperatorCode("DISTR") );     //null
    }
}
