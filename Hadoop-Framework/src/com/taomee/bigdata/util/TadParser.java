package com.taomee.bigdata.util;

import java.util.HashSet;

public class TadParser {
    private static HashSet<String> ends = new HashSet<String>();
    static {
        ends.add("co");
        ends.add("com");
        ends.add("cn");
        ends.add("net");
    }

    private static boolean isIP(String items[]) {
        if(items.length != 4)   return false;
        for(int i=0; i<4; i++) {
            try {
                Integer.valueOf(items[i]);
            } catch (java.lang.NumberFormatException e) {
                return false;
            }
        }
        return true;
    }

    public static String parser(String tad) {
        tad = tad.toLowerCase().replaceAll("\\s*", "");
        tad = tad.replaceAll("http@", "http:");
        tad = tad.replaceAll("@", "=");
        if(tad.compareTo("none") == 0) {
            return "未加载到js";
        } else if(tad.compareTo("unknown") == 0) {
            return "直接进入游戏";
        } else if(tad.startsWith("#http") || tad.startsWith("http")) {
            try {
                tad = java.net.URLDecoder.decode(tad, "utf-8");
            } catch (java.io.UnsupportedEncodingException e) {
            } catch (java.lang.IllegalArgumentException e) {
                try {
                    tad = tad.substring(0, tad.lastIndexOf('%'));
                    tad = java.net.URLDecoder.decode(tad, "utf-8");
                } catch (java.io.UnsupportedEncodingException e1) {
                } catch (java.lang.IllegalArgumentException e2) {
                        tad = Escape.unescape(tad);
                    //try {
                    //} catch (java.io.UnsupportedEncodingException e3) {
                    //}
                }
            }
            String items[] = tad.split("/");
            //System.out.println(tad);
            if(items.length < 2) {
                //System.err.println("wrong URL");
                return "URL解析错误";
            } else {
                items = items[2].split("[:=]")[0].split("[.]");
                if(items.length < 2 || isIP(items)) return "URL解析错误";
                for(int i=1; i<items.length; i++) {
                    if(ends.contains(items[i])) {
                        return items[i-1]+"."+items[i];
                    }
                }
                return items[items.length-2]+"."+items[items.length-1];
            }
        } else {
            String items[] = tad.split("[.]");
            if(items.length != 4) {
                return "未知";
            } else {
                return items[0];
            }
        }
    }

    public static void main(String args[]) {
        System.out.println(TadParser.parser(args[0]));
    }
}
