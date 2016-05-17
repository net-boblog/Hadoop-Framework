package com.taomee.bigdata.transfer;

import java.lang.StringBuffer;

public class TransferToSource extends TransferTo {

    protected void conf(String[] argv) {

    }

    protected String transferStat(StatInfo stat) {
        StringBuffer buffer = new StringBuffer();
        buffer.append(String.format("_hip_=127.0.0.1\t_stid_=%s\t_sstid_=%s\t_gid_=%d\t_zid_=%d\t_sid_=%d\t_pid_=%s\t_ts_=%d\t_acid_=%s\t_plid_=%s",
                    stat.getStid(), stat.getSstid(),
                    stat.getGame(), stat.getZone(), stat.getServer(), stat.getPlatform(),
                    stat.getTime(), stat.getUid(), stat.getPid()));
        if(stat.getOp() != null) {
            buffer.append(String.format("\t%s\t_op_=%s",
                        stat.getField(), stat.getOp()));
        }
        return buffer.toString();
    }
}
