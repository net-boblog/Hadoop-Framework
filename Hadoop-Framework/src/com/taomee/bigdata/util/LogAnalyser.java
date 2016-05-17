package com.taomee.bigdata.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.sql.ResultSet;
import java.sql.SQLException;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.util.Tool;
import com.taomee.bigdata.lib.ReturnCode;
import com.taomee.bigdata.util.MysqlConnection;

public class LogAnalyser extends Configured 
{
    //                                            [0]       [1]       [2]       [3]      [4]      [5]      [6]     [7]      [8]
    private final static String[] basicKeys = { "_hip_", "_stid_", "_sstid_", "_gid_", "_pid_", "_zid_", "_sid_", "_ts_", "_acid_" };//所有统计项都要有的key
    public final static String HOST_IP   = basicKeys[0];    //落统计log的服务器IP
    public final static String STAT_ID   = basicKeys[1];    //统计项ID
    public final static String STID      = basicKeys[1];    //统计项ID
    public final static String SSTAT_ID  = basicKeys[2];    //子统计项ID
    public final static String SSTID     = basicKeys[2];    //子统计项ID
    public final static String GAME      = basicKeys[3];    //游戏名
    public final static String PLATFORM  = basicKeys[4];    //运营平台
    public final static String ZONE      = basicKeys[5];    //大区
    public final static String SERVER    = basicKeys[6];    //服务器
    public final static String TIME      = basicKeys[7];    //时间戳
    public final static String ACCOUNT   = basicKeys[8];    //账号
    public final static String ROLE      = "_plid_";        //角色ID
    public final static String CLIENT_IP = "_cip_";         //客户端IP
    public final static String OP        = "_op_";          //操作符

    public final static String KEY_AMT = "_amt_";			//付费额
    public final static String KEY_LV = "_lv_";				//等级
    
    private HashMap<String, String> keyValueSet = new HashMap<String, String>(); //保存源数据log
    private HashMap<String, String> msgValueSet = new HashMap<String, String>(); //保存关系表数据
    private ReturnCode code = ReturnCode.get();
    private String log = null;
    private OperatorAnalyser opAnalyser = new OperatorAnalyser();
    private ArrayList<String[]> output = new ArrayList<String[]>(64 * 1024);

	private Configuration conf = null;

	//从job中将conf传入，方便调取配置文件中的信息
	public void transConf(Configuration cf)
	{
		conf = cf; 
	}

    public int queryGetMsg() {
        if(conf == null)    return code.setCode("E_GET_MSGINFO_CONF_NOT_SET");
		msgValueSet.clear();
		MysqlConnection mysql = new MysqlConnection();
        String mysqlUrl = conf.get("mysql.url");
        String mysqlUser = conf.get("mysql.user");
        String mysqlPasswd = conf.get("mysql.passwd");
		mysql.connect(mysqlUrl, mysqlUser, mysqlPasswd);

		if(mysql == null) {
			throw new RuntimeException(String.format("url=[%s] user=[%s] pwd=[%s]",
						conf.get("mysql.url"), conf.get("mysql.user"), conf.get("mysql.passwd")));
		}   
		ResultSet rSet = mysql.doSql("SELECT CONCAT(id,'-',game_id),type,first,second,third,fourth FROM t_custom_msgid_info");
        if(rSet == null) {
			return code.setCode("E_GET_MSGINFO_FROM_DB_DOSQL");
        }  
		try {
			while(rSet.next()) {
				String value = String.format("%s\t%s\t%s\t%s\t%s", 
						rSet.getString(2), rSet.getString(3), rSet.getString(4), rSet.getString(5), rSet.getString(6));
				//id和game_id共同为主键，key表示成：id-gameid
				String key = rSet.getString(1).trim();
				msgValueSet.put(key, value);
			}   
		} catch (SQLException e) {
			return code.setCode("E_GET_MSGINFO_FROM_DB_GVALUE", e.getMessage());
		}   
		mysql.close();
		return code.setOkCode();
	}

    public int analysis(String log) {
        keyValueSet.clear();
        output.clear();
        this.log = new String(log);

        String[] keyValue = log.trim().split("\t"); //将log按'\t'分段,存入字符串数组keyValue
        if(keyValue == null || keyValue.length == 0) {
            return code.setCode("W_LOG_EMPTY");
        }

        //获取每个字段
        for(int i=0; i<keyValue.length; i++) {
			//例：keyValue[1]:_stid_=msgid_ keyValue[2]:_sstid_=123456
            String[] kv = keyValue[i].split("=");
			//例：kv[0]:_stid_ kv[1]:msgid_
            if(kv == null || kv.length < 2) {
                code.setCode("E_KV_FORMAT", String.format("[%s][%s]", log, keyValue[i]));
                keyValueSet.put(kv[0].trim(), "-1");
                continue;
            }

            if(kv.length > 2) {
                for(int j=2; j<kv.length; j++) {
                    kv[1] += ('=' + kv[j]);
                }
            }

            //if(keyValueSet.containsKey(kv[0])) {SDK已经判断，不会出现次情况
            //    return code.setCode("E_KEY_EXIST", String.format("[%s][%s]", log, keyValue[i]));
            //}

            keyValueSet.put(kv[0].trim(), kv[1].trim());
        }

		//检测到_stid_=msgid_,需要对统计项进行转换
		if(keyValueSet.containsKey("_stid_") && keyValueSet.get("_stid_").compareToIgnoreCase("msgid_") == 0) {
			//检查缓存空间，只缓存一次
			if(msgValueSet.isEmpty()) {
				int ret = queryGetMsg();
				if(!code.isOK(ret)) {
					return ret;
				}
			}
			//源数据中获取msgid和gid，共同作为key去缓存中查找
			String msgKey = keyValueSet.get("_sstid_") + "-" + keyValueSet.get("_gid_");
			String op = new String();
			//若缓存中没有找到该msgKey，直接返回错误
			if(!msgValueSet.containsKey(msgKey)){
				return code.setCode("E_GET_MSGID_NONE");
			}
			String msgvalue = msgValueSet.get(msgKey);
			//type,first,second,third,fourth
			//[0]     [1]  [2]   [3]    [4]
			String[] msgkv = msgvalue.trim().split("\t");
			Integer type = Integer.valueOf(msgkv[0]);
			String first = msgkv[1];
			String second = msgkv[2];
			String third = new String();
			String fourth = new String();
			if(msgkv.length == 4){
				third = msgkv[3];
			}
			else if(msgkv.length == 5){
				third = msgkv[3];
				fourth = msgkv[4]; 
			}
			keyValueSet.put("_stid_", first);
			keyValueSet.put("_sstid_", second);
            //人数人次
            if(type == 1 && !third.isEmpty()) {
                keyValueSet.put("item", third);
                op = "item:item";
                keyValueSet.put("_op_", op);
            }
            //求和
            else if(type == 2 && !third.isEmpty() && fourth.isEmpty()) {
                if(keyValueSet.containsKey("_value_")){
                    keyValueSet.put(third, keyValueSet.get("_value_"));
                }
                else{
                    keyValueSet.put(third, "0");
                }
                op = "sum:";
                op += third;
                keyValueSet.put("_op_", op);
            }
            else if(type == 2 && !third.isEmpty() && !fourth.isEmpty()) {
                if(keyValueSet.containsKey("_value_")){
                    keyValueSet.put(fourth, keyValueSet.get("_value_"));
                }
                else{
                    keyValueSet.put(fourth, "0");
                }
                keyValueSet.put("item", third);
                op = "item_sum:item,";
                op += fourth;
                keyValueSet.put("_op_", op);
            }
            //最大值
            else if(type == 3 && !third.isEmpty() && fourth.isEmpty()) {
                if(keyValueSet.containsKey("_value_")){
                    keyValueSet.put(third, keyValueSet.get("_value_"));
                }
                else{
                    keyValueSet.put(third, "0");
                }
                op = "max:";
                op += third;
                keyValueSet.put("_op_", op);
            }
            else if(type == 3 && !third.isEmpty() && !fourth.isEmpty()) {
                if(keyValueSet.containsKey("_value_")){
                    keyValueSet.put(fourth, keyValueSet.get("_value_"));
                }
                else{
                    keyValueSet.put(fourth, "0");
                }
                keyValueSet.put("item", third);
                op = "item_max:item,";
                op += fourth;
                keyValueSet.put("_op_", op);
            }
		}

        //检查是否包含了必须的字段
        if(!checkBasicKeys()) {
            return code.getLastCode();
        }

        try {
            Integer.valueOf(keyValueSet.get(GAME));
        } catch (NumberFormatException e) {
            return code.setCode("E_LOG_GAMEID_NOTNUM");
        }
        try {
            Integer.valueOf(keyValueSet.get(PLATFORM));
        } catch (NumberFormatException e) {
            return code.setCode("E_LOG_PLATFORMID_NOTNUM");
        }
        try {
            Integer.valueOf(keyValueSet.get(ZONE));
        } catch (NumberFormatException e) {
            return code.setCode("E_LOG_ZONEID_NOTNUM");
        }
        try {
            Integer.valueOf(keyValueSet.get(SERVER));
        } catch (NumberFormatException e) {
            return code.setCode("E_LOG_SERVERID_NOTNUM");
        }

        return code.setOkCode();
    }


    public int analysisAndGet(String log) {
        int ret = analysis(log);
        if(!code.isOK(ret)) {
            return ret;
        }
        opAnalyser.analysis(keyValueSet.get(OP));

        while(opAnalyser.hasNext()) {
            //输出格式  op time [value] account[-player] game platform zone server stid sstid [op-field] [key]
            String[] nextOp = opAnalyser.next();
            String[] zs = combine(getValue(ZONE), getValue(SERVER), getValue(PLATFORM));
            String ap = getAPid();
            for(int i=0; i<zs.length; i++) {
                if(nextOp == null) {
                    break;
                }
                if(nextOp.length == 1) {//UCOUNT,COUNT
                    output.add(
                            new String[] {
                                nextOp[0].toUpperCase(),
                                getValue(TIME),
                                null,
                                String.format("%s\t%s\t%s\t%s\t%s", ap, getValue(GAME), zs[i], getValue(STAT_ID), getValue(SSTAT_ID))
                            });
                } else if(nextOp.length == 2) {//SUM,MAX,SET,IP_DISTR,DISTR*
                    if(nextOp[0].compareToIgnoreCase("item") == 0) {
                        output.add(
                                new String[] {
                                    "UCOUNT",
                                    getValue(TIME),
                                    null,
                                    String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s", ap, getValue(GAME), zs[i], getValue(STAT_ID), getValue(SSTAT_ID), nextOp[1], getValue(nextOp[1]))
                                });
                        output.add(
                                new String[] {
                                    "COUNT",
                                    getValue(TIME),
                                    null,
                                    String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s", ap, getValue(GAME), zs[i], getValue(STAT_ID), getValue(SSTAT_ID), nextOp[1], getValue(nextOp[1]))
                                });
                    } else {
                        output.add(
                                new String[] {
                                    nextOp[0].toUpperCase(),
                                    getValue(TIME),
                                    getValue(nextOp[1]),
                                    String.format("%s\t%s\t%s\t%s\t%s\t%s", ap, getValue(GAME), zs[i], getValue(STAT_ID), getValue(SSTAT_ID), nextOp[1])
                                });
                    }
                } else {//ITEM_SUM,ITEM_MAX,ITEM_SET,ITEM_DISTR
                    output.add(
                            new String[] {
                                nextOp[0].toUpperCase(),
                                getValue(TIME),
                                getValue(nextOp[2]),
                                String.format("%s\t%s\t%s\t%s\t%s\t%s,%s\t%s", ap, getValue(GAME), zs[i], getValue(STAT_ID), getValue(SSTAT_ID), nextOp[1], nextOp[2], getValue(nextOp[1]))
                            });
                }
            }
        }

        return code.setOkCode();
    }
    
    //原始日志解析为:<k,v>
    public int parse(String log) {
        keyValueSet.clear();
        output.clear();
        this.log = log;

        String[] keyValue = log.trim().split("\t");
        if(keyValue == null || keyValue.length == 0) {
            return code.setCode("W_LOG_EMPTY");
        }

        //获取每个字段
        for(int i=0; i<keyValue.length; i++) {
            if(keyValue[i].trim().length() == 0)    continue;
            String[] kv = keyValue[i].split("=");
            if(kv == null || kv.length != 2) {
                return code.setCode("E_KV_FORMAT", String.format("[%s][%s]", log, keyValue[i]));
            }

            if(keyValueSet.containsKey(kv[0].trim())) {
                return code.setCode("E_KEY_EXIST", String.format("[%s][%s]", log, keyValue[i]));
            }

            keyValueSet.put(kv[0].trim(), kv[1].trim());
        }

        //检查是否包含了必须的字段
        if(!checkBasicKeys()) {
            return code.getLastCode();
        }

        return code.setOkCode();
    }
    
    /**return gid zid sid pid apid*/
    public String getKey() {        
        return String.format("%s\t%s\t%s\t%s\t%s", 
        					getValue(GAME)
        				   ,getValue(ZONE)
        				   ,getValue(SERVER)
        				   ,getValue(PLATFORM)
        				   ,getAPid());
    }
    
    /**return gid zid sid pid [ext_key] apid*/
    public String getExtKey(String extKey) {        
        return String.format("%s\t%s\t%s\t%s\t%s\t%s", 
        					getValue(GAME)
        				   ,getValue(ZONE)
        				   ,getValue(SERVER)
        				   ,getValue(PLATFORM)
        				   ,getValue(extKey)
        				   ,getAPid());
    }

    public String getValue(String key) {
        if(key == null) {
            code.setCode("W_KEY_EMPTY", String.format("[%s][%s]", log, key));
            return null;
        }
        String ret = keyValueSet.get(key);
        if(ret == null) {
            code.setCode("E_KEY_NOT_EXIST", String.format("[%s][%s]", log, key));
        } else {
            code.setOkCode();
        }
        return ret;
    }

	//获取所有统计字段
	public String getAllValue() {
		Iterator it = keyValueSet.keySet().iterator(); 
		String ret = new String();
        while(it.hasNext()) {  
			String key = (String)it.next();  
			ret += (key + "=" + keyValueSet.get(key) + "\t");
        }   
		return ret;
	}
	

    public boolean containsKey(String key) {
        return keyValueSet.containsKey(key);
    }

    public ArrayList<String[]> getOutput() {
        return output;
    }

    private boolean checkBasicKeys() {
        for(int i=0; i<basicKeys.length; i++) {
            if(!keyValueSet.containsKey(basicKeys[i])) {
                code.setCode("E_BASIC_KEY_NOT_EXIST", String.format("[%s][%s]", log, basicKeys[i]));
                return false;
            }
        }
        return true;
    }

    public String[] combine() {
        return combine(getValue(ZONE), getValue(SERVER), getValue(PLATFORM));
    }

    private String[] combine(String zone, String server, String platform) {
        if(zone.compareTo("-1") == 0 &&
                server.compareTo("-1") == 0) {
            if(platform.compareTo("-1") == 0) {
                return new String[]{"-1\t-1\t-1"};
            } else {
                return new String[]{ "-1\t-1\t" + platform,
                                "-1\t-1\t-1"};
            }
        }
        if(platform.compareTo("-1") == 0) {
            return new String[]{ zone + "\t" + server + "\t-1",
                                "-1\t-1\t-1" };
        }
        return new String[]{ zone + "\t" + server + "\t" + platform,
                         zone + "\t" + server + "\t-1",
                         "-1\t-1\t" + platform,
                         "-1\t-1\t-1" };
    }

    public String getAPid() {
        String account = getValue(ACCOUNT);
        String role = keyValueSet.get(ROLE);
        if(role != null) {
            return account + "-" + role;
        }
        return account + "--1";
    }

    public static void main(String args[]) {
        String[] log = new String[] {
//            "_hip=10.1.1.63\t_stid_=active\t_sstid_=active\t_gid_=seer\t_pid_=taomee\t_zid_=0\t_sid_=1\t_ts_=1383117270\t_acid_=185908545",
//            "_hip=10.1.1.63\t_stid_=active\t_sstid_=active\t_gid_=seer\t_pid_=taomee\t_zid_=0\t_sid_=1\t_ts_=1383117270\t_acid_=185908545\t_plid_=1383117270",
            "_hip=10.1.1.63\t_stid_=active\t_sstid_=active\t_gid_=seer\t_pid_=taomee\t_zid_=0\t_sid_=1\t_ts_=1383117270\t_acid_=185908545\tcoins=10\t_op_=sum:coins",
            "_hip=10.1.1.63\t_stid_=active\t_sstid_=active\t_gid_=seer\t_pid_=taomee\t_zid_=0\t_sid_=1\t_ts_=1383117270\t_acid_=185908545\tproduct=1\tcoins=10\t_plid_=1383117270\t_op_=sum:coins|item:product|item_sum:product,coins",
        };
        LogAnalyser l = new LogAnalyser();
        ReturnCode rCode = ReturnCode.get();
        for(int i=0; i<log.length; i++) {
            l.analysis(log[i]);
            System.out.println("error:" + rCode.getErrorList());
            System.out.println("warn:" + rCode.getWarnList());
            ArrayList<String[]> o = l.getOutput();
            System.out.println(log[i]);
            for(int j=0; j<o.size(); j++) {
                System.out.println(o.get(j)[0] + "\t" + o.get(j)[1] + "\t" + o.get(j)[2] + "\t" + o.get(j)[3]);
            }
        }
    }

}
