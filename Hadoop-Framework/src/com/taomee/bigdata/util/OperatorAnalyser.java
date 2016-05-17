package com.taomee.bigdata.util;

import com.taomee.bigdata.lib.ReturnCode;
import com.taomee.bigdata.lib.Operator;

import java.util.HashSet;
import java.util.Iterator;

class OperatorAnalyser
{
    private String op;
    private ReturnCode rCode = ReturnCode.get();
    private HashSet<String> opSet = new HashSet<String>();
    private Iterator<String> it;
    private int iterator;

    public int analysis(String op) {
        opSet.clear();
        it = null;
        iterator = 0;
        this.op = op;
        //默认所有数据都会计算人数人次
        opSet.add("ucount");
        opSet.add("count");
        if(op != null ) {
            String[] items = op.split("[|]");
            for(int i=0; i<items.length; i++) {
                opSet.add(items[i].trim());
            }
        }
        it = opSet.iterator();
        return rCode.setOkCode();
    }

    public boolean hasNext() {
        if(it != null)  return it.hasNext();
        return false;
    }

    public String[] next() {
        if(it == null) {
            rCode.setCode("E_OP_SPLIT_EMPTY", String.format("[%s] split empty", op));
            return null;
        }

        String nextOp = it.next();
        iterator ++;
        if(nextOp == null) {
            rCode.setCode("W_NEXT_OP_EMPTY", String.format("[%s] get [%d] op empty", op, iterator));
            return null;
        } else {
            //解析出op:item_id,value
        	//"_hip=10.1.1.63\t_stid_=active\t_sstid_=active\t_gid_=seer\t_pid_=taomee\t_zid_=0\t_sid_=1\t_ts_=1383117270\t_acid_=185908545\tproduct=1\tcoins=10\t_plid_=1383117270\t
        	//_op_=sum:coins|item:product|item_sum:product,coins",
            if(Operator.UCOUNT == Operator.getOperatorCode(nextOp)) {
                return new String[]{ "UCOUNT" };
            }
            if(Operator.COUNT == Operator.getOperatorCode(nextOp)) {
                return new String[]{ "COUNT" };
            }
            String[] tmp = nextOp.split(":");
            if(tmp == null || tmp.length != 2) {
                rCode.setCode("E_OP_FORMAT_ERROR", String.format("[%s][%s]", op, nextOp));
                return null;
            }
            if(!Operator.isOperator(tmp[0])) {
                rCode.setCode("E_NOT_OPERATOR", String.format("[%s][%s]", op, tmp[0]));
                return null;
            }
            rCode.setOkCode();
            Integer opCode = Operator.getOperatorCode(tmp[0]);
            String kv[] = tmp[1].split(",");
            if(opCode == Operator.ITEM_SUM
                    || opCode == Operator.ITEM_MAX
                    || opCode == Operator.ITEM_SET
                    || opCode == Operator.ITEM_DISTR) {
                if(kv == null || kv.length != 2) {
                    rCode.setCode("E_OP_FORMAT_ERROR", String.format("[%s][%s]", op, tmp[1]));
                    return null;
                }
                String[] ret = new String[3];
                if(opCode == Operator.ITEM_DISTR) {
                    ret[0] = "DISTR_SET";
                } else {
                    ret[0] = tmp[0].substring(5);
                }
                ret[1] = kv[0];
                ret[2] = kv[1];
                return ret;
            } else {
                if(kv == null || kv.length != 1) {
                    rCode.setCode("E_OP_FORMAT_ERROR", String.format("[%s][%s]", op, tmp[1]));
                    return null;
                }
                String[] ret = new String[2];
                ret[0] = tmp[0];
                ret[1] = kv[0];
                return ret;
            }
        }
        
    }

}
