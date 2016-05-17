package com.taomee.bigdata.lib;

import com.taomee.bigdata.lib.ReturnCode;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Distr
{
    public static String getDistrName(Integer distr[], int index) {
        return getDistrName(distr, index, 1);
    }

    public static String getDistrName(Integer distr[], int index, int ratio)
    {
        ReturnCode r = ReturnCode.get();
        if(distr == null) {
            return Integer.toString(index/ratio);
        }
        if(index < 0 || index > distr.length) {
            r.setCode("E_USERONLINE_SUMREDUCER_DISTR", String.format("index[%d] illegal, should be [0,%d]", index, distr.length));
            return null;
        }
        if(index == distr.length) {
            return index + ":>=" + distr[distr.length-1]/ratio;
        }
        if(index == 0) {
            if(distr[0]/ratio <= 1) {
                return "0:0";
            } else if(distr[0]/ratio <= 2) {
                return "0:1";
            } else {
                return "0:<" + distr[0]/ratio;
            }
        }
        return String.format("%d:[%d,%d)", index, distr[index-1]/ratio, distr[index]/ratio);
    }

    public static int getRangeIndex(Integer distr[], Integer value) {
        if(distr == null)   return value;
        for(int i=0; i<distr.length; i++) {
            if(value < distr[i])   return i;
        }
        return distr.length;
    }

    public static int getRangeIndex(Integer distr[], Double value) {
        if(distr == null)   return value.intValue();
        for(int i=0; i<distr.length; i++) {
            if(value.intValue() <= distr[i])   return i;
        }
        return distr.length;
    }

    public static Integer[] getDistrFromResult(ResultSet rSet) {
        if(rSet == null)    return null;
        try {
            rSet.last();
            if(rSet.getRow() == 0)  return null;
            Integer ret[] = new Integer[rSet.getRow()];
            rSet.beforeFirst();//ResultSet 指针最初位于第一行之前；第一次调用 next 方法使第一行成为当前行；第二次调用使第二行成为当前行，依此类推。
            int i = 0;
            while(rSet.next()) {
                ret[i++] = rSet.getInt(1);
            }
            return ret;
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            return null;
        }
    }
}
