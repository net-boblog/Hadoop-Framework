package com.taomee.bigdata.lib;

import java.util.HashMap;
import java.util.Iterator;
import java.util.ArrayList;
import java.lang.Thread;
import java.lang.Class;
import java.lang.reflect.Method;
import java.util.Date;
import java.text.SimpleDateFormat;

public final class ReturnCode implements Cloneable
{
    public static final int G_OK = 0;
    public static final int UNKNOW = 1;
    public static final int UNDEFINED = 2;

    public static final int E_MIN = 0x1000;
    public static final int ERROR = E_MIN;
    public static final int E_MAX = 0x1fffffff;

    public static final int W_MIN = 0x20000000;
    public static final int WARNING  = W_MIN;
    public static final int W_MAX = 0x2fffffff;

    public static final int D_MIN = 0x30000000;
    public static final int DEBUG = D_MIN;
    public static final int D_MAX = 0x3fffffff;

    public static final int S_MIN = 0x40000000;

    private static final HashMap<String, String> type = new HashMap<String, String>();

    public static boolean isOK(int code)       { return code == G_OK; }
    public static boolean isError(int code)    { return E_MIN <= code && code <= E_MAX; }
    public static boolean isWarn(int code)     { return W_MIN <= code && code <= W_MAX; }
    public static boolean isDebug(int code)    { return D_MIN <= code && code <= D_MAX; }

    private int errorCode;
    private String errorInfo;
    private ArrayList<String> errorList = new ArrayList<String>(100);
    private ArrayList<String> warnList  = new ArrayList<String>(100);
    private ArrayList<String> debugList  = new ArrayList<String>(100);
    private ArrayList<String> elseList  = new ArrayList<String>(100);

    private static final HashMap<String, Integer> error =    new HashMap<String, Integer>(); 
    private static final HashMap<String, Integer> warning =  new HashMap<String, Integer>(); 
    private static final HashMap<String, Integer> debug =  new HashMap<String, Integer>(); 
    private static final HashMap<String, Integer> elseCase = new HashMap<String, Integer>();
    private static final HashMap<Integer, String> _error =   new HashMap<Integer, String>(); 
    private static final HashMap<Integer, String> _warning = new HashMap<Integer, String>(); 
    private static final HashMap<Integer, String> _debug = new HashMap<Integer, String>(); 
    private static final HashMap<Integer, String> _elseCase = new HashMap<Integer, String>();

    private static final HashMap<Long, ReturnCode> oSet = new HashMap<Long, ReturnCode>();
    private static final SimpleDateFormat df = new SimpleDateFormat("[yyyy-MM-dd HH:mm:ss.SSS]");

    private ReturnCode() {
        type.put("ERROR", "getErrorList");
        type.put("WARNING", "getWarnList");
        type.put("ELSE", "getElseList");
        type.put("DEBUG", "getDebugList");
    }

    protected Object clone() { return get(); }

    public static ReturnCode get() {
        Long tid = Thread.currentThread().getId();
        ReturnCode ret = oSet.get(tid);
        if(ret == null) {
            ret = new ReturnCode();
            oSet.put(tid, ret);
        }
        return ret;
    }

    public static int getCodeType(int code) {
        if(code == G_OK) {
            return G_OK;
        } else if(code < E_MIN) {
            return UNKNOW;
        } else if(code <= E_MAX) {
            return ERROR;
        } else if(code <= W_MAX) {
            return WARNING;
        } else if(code <= D_MAX) {
            return DEBUG;
        } else {
            return UNDEFINED;
        }
    }

    public int setOkCode() {
        return errorCode = G_OK;
    }

    public int setCode(String code, String info) {
        if(code.toUpperCase().startsWith("E_")) {
            return errorCode = setErrorCode(code, info);
        } else if(code.toUpperCase().startsWith("W_")) {
            return errorCode = setWarnCode(code, info);
        } else if(code.toUpperCase().startsWith("D_")) {
            return errorCode = setDebugCode(code, info);
        } else if(code.compareTo("G_OK") == 0){
            return errorCode = G_OK;
        } else {
            return errorCode = setElseCode(code, info);
        }
    }

    private int setErrorCode(String code, String info) {
        errorInfo = info;
        Integer c = error.get(code);
        if(c == null) {
            error.put(code, error.size() + E_MIN);
            c = error.get(code);
            _error.put(c, code);
        }
        errorList.add(df.format(new Date()) + getErrorString(c));
        return c;
    }

    private int setWarnCode(String code, String info) {
        errorInfo = info;
        Integer c = warning.get(code);
        if(c == null) {
            c = warning.put(code, warning.size() + W_MIN);
            c = warning.get(code);
            _warning.put(c, code);
        }
        warnList.add(df.format(new Date()) + getErrorString(c));
        return c;
    }

    private int setDebugCode(String code, String info) {
        errorInfo = info;
        Integer c = debug.get(code);
        if(c == null) {
            c = debug.put(code, debug.size() + D_MIN);
            c = debug.get(code);
            _debug.put(c, code);
        }
        debugList.add(df.format(new Date()) + getErrorString(c));
        return c;
    }

    private int setElseCode(String code, String info) {
        errorInfo = info;
        Integer c = elseCase.get(code);
        if(c == null) {
            c = elseCase.put(code, elseCase.size() + S_MIN);
            c = elseCase.get(code);
            _elseCase.put(c, code);
        }
        elseList.add(df.format(new Date()) + getErrorString(c));
        return c;
    }

    public int setCode(String code) {
        if(code.toUpperCase().startsWith("E_")) {
            return errorCode = setErrorCode(code, null);
        } else if(code.toUpperCase().startsWith("W_")) {
            return errorCode = setWarnCode(code, null);
        } else if(code.toUpperCase().startsWith("D_")) {
            return errorCode = setDebugCode(code, null);
        } else if(code.compareTo("G_OK") == 0){
            errorCode = G_OK;
            errorInfo = String.format("[%s]", code);
            return G_OK;
        } else {
            return errorCode = setElseCode(code, null);
        }
    }

    private int setErrorCode(String code) {
        return setErrorCode(code, null);
    }

    private int setWarnCode(String code) {
        return setWarnCode(code, null);
    }

    private int setDebugCode(String code) {
        return setDebugCode(code, null);
    }

    public String getErrorString(int errorCode)
    {
        String code;
        if(isOK(errorCode)) {
            return "success";
        } else if(isError(errorCode)) {
            code = _error.get(errorCode);
            if(code == null) {
                return String.format("0x%08x error [no description]", errorCode);
            } else {
                return String.format("0x%08x error [%s] : %s", errorCode, code, errorInfo);
            }
        } else if(isWarn(errorCode)) {
            code = _warning.get(errorCode);
            if(code == null) {
                return String.format("0x%08x warning [no description]", errorCode);
            } else {
                return String.format("0x%08x warning [%s] : %s", errorCode, code, errorInfo);
            }
        } else if(isDebug(errorCode)) {
            code = _debug.get(errorCode);
            if(code == null) {
                return String.format("0x%08x debug [no description]", errorCode);
            } else {
                return String.format("0x%08x debug [%s] : %s", errorCode, code, errorInfo);
            }
        } else {
            code = _elseCase.get(errorCode);
            if(code == null) {
                return String.format("0x%08x undefined [no description]", errorCode);
            } else {
                return String.format("0x%08x undefined [%s] : %s", errorCode, code, errorInfo);
            }
        }
    }

    public String getLastInfo() {
        return getErrorString(errorCode);
    }

    public int getLastCode() {
        return errorCode;
    }

    public ArrayList<String> getErrorList() {
        return errorList;
    }

    public ArrayList<String> getWarnList() {
        return warnList;
    }

    public ArrayList<String> getDebugList() {
        return debugList;
    }

    public ArrayList<String> getElseList() {
        return elseList;
    }

    public HashMap<String, String> getType() {
        return type;
    }

    public static void main(String[] args) {
        ReturnCode r = ReturnCode.get();
        r.setCode("E_!", "111");
        r.setCode("E_d", "121");
        r.setCode("E_d", "121");
        r.setCode("E_hd89", "123");
        r.setCode("W_hd89", "123");
        r.setCode("W_hd89", "123");
        r.setCode("W_7845yih", "124");
        r.setCode("E_d");
        r.setCode("E_d");
        r.setCode("E_hd89");
        r.setCode("W_hd89");
        r.setCode("W_hd89");
        r.setCode("_hd89", "123");
        r.setCode("_hd89", "123");
        r.setCode("_hd89", "123");
        r.setCode("_7845yih", "124");
        r.setCode("_d");
        r.setCode("_d");
        r.setCode("_hd89");
        r.setCode("_hd89");
        r.setCode("_hd89");
        HashMap<String, String> h = r.getType();
        Iterator<String> it = h.keySet().iterator();
        while(it.hasNext()) {
            Method m = null;
            try {
                m = r.getClass().getMethod(h.get(it.next()));
                ArrayList<String> a = (ArrayList<String>)m.invoke(r);
                for(int i=0; i<a.size(); i++) {
                    System.out.println(a.get(i));
                }
            } catch (java.lang.NoSuchMethodException e) {
                System.out.println(e.getMessage());
            } catch (java.lang.IllegalAccessException e) {
                System.out.println(e.getMessage());
            } catch (java.lang.reflect.InvocationTargetException e) {
                System.out.println(e.getMessage());
            }
        }
    }

}
