package com.taomee.bigdata.util;

import java.util.*;
import java.sql.*;

public class IPDistrCity
{
    private class IPRange implements Comparable<IPRange>
    {
        private long startIP;
        private long endIP;
        private String provinceCity;


        public long getStartIP()
        {
            return this.startIP;
        }

        public long getEndIP()
        {
            return this.endIP;
        }

        public String getProvinceCity()
        {
            return this.provinceCity;
        }

        public IPRange(long startIP, long endIP, String provinceCity)
        {
            this.startIP = startIP;
            this.endIP = endIP;
			this.provinceCity = provinceCity;
        }

        public int compareTo(IPRange other)
        {
            long otherStartIP = other.getStartIP();
            long otherEndIP = other.getEndIP();

            if (this.startIP > otherStartIP) {
                return 1;
            } else if (this.startIP == otherStartIP) {
                if (this.endIP > otherEndIP) {
                    return 1;
                } else if (this.endIP == otherEndIP) {
                    return 0;
                } else {
                    return -1;
                }
            } else {
                return -1;
            }
        }
    }

    private class IPRangeComparator implements Comparator<IPRange>
    {
        public int compare(IPRange first, IPRange second)
        {
            return first.compareTo(second);
        }
    }

    public long convertToIPHost(long ip)
    {
        long result = 0;

        result |= (ip >> 24) & 0xFF;
        result |= ((ip >> 16) & 0xFF) << 8;
        result |= ((ip >> 8) & 0xFF) << 16;
        result |= (ip & 0xFF) << 24;

        return result;
    }

    private Connection mysqlConn = null;
    private IPRange[] ipRangeListCity = null;

    public IPDistrCity(Connection mysqlConn) throws SQLException
    {
        this.mysqlConn = mysqlConn;
        init(this.mysqlConn);
    }

    private void init(Connection mysqlConn) throws SQLException
    {
        Statement statement = mysqlConn.createStatement();
        ResultSet result = null;

        ArrayList<IPRange> list = new ArrayList<IPRange>();

		//ip精确到城市
        result = statement.executeQuery(
				"select start_ip, end_ip, city from t_city_ip_2015_Q3 where city like '%杭州%' or city like '%温州%' or city like '%东阳%' or city like '%义乌%' or city like '%嘉兴%' or city like '%诸暨%' or city like '%宁波%' or city like '%金华%' or city like '%苏州%' or city like '%无锡%' or city like '%南京%' or city like '%镇江%' or city like '%南通%' or city like '%扬州%' or city like '%徐州%' or city like '%常州%' or city like '%郑州%' or city like '%许昌%' or city like '%洛阳%' or city like '%平顶山%' or city like '%周口%'");
        while (result.next()) {
            long startIP = result.getLong(1);
            long endIP = result.getLong(2);
            String cityName = result.getString(3);
			
			//String provinceCity = provinceName + ":" + cityName;

            list.add(new IPRange(startIP, endIP, cityName));
        }

        ipRangeListCity = new IPRange[list.size()];
        list.toArray(ipRangeListCity);
        Arrays.sort(ipRangeListCity, new IPRangeComparator());

    }

    /**
     * @brief construct function, receive one mysql schema uri
     * @param  mysqluri ::= mysql://user:password@<host/ip>[:port]/database[?options]
     */
    public IPDistrCity(String mysqluri) throws ClassNotFoundException, SQLException
    {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            System.err.println("not find mysql lib");
            e.printStackTrace();
            throw e;
        }

        MysqlUriParser myp = new MysqlUriParser(mysqluri);
        try {
            this.mysqlConn = DriverManager.getConnection(myp.getJdbcURL(), myp.getUser(), myp.getPassword());
        } catch (SQLException e) {
            System.err.println("get connection failed");
            e.printStackTrace();
            throw e;
        }

        init(this.mysqlConn);
    }

	//获取ip范围
    public IPRange getHitRangeCity(long ip, boolean needConvert)
    {
        int low = 0;
        int high = ipRangeListCity.length - 1;
        int mid = 0;
        long ipHost = needConvert ? this.convertToIPHost(ip) : ip;

        while (low <= high) {
            mid = (low + high) / 2;
            if (ipHost >= ipRangeListCity[mid].getStartIP()) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }

        if (high < 0 || high >= ipRangeListCity.length) {
            return null;
        }

        IPRange hit = ipRangeListCity[high];
        if (ipHost < hit.getStartIP() || ipHost > hit.getEndIP()) {
            return null;
        }

        return hit;
    }

	//获取城市名
    public String getIPProvinceCity(long ip, boolean needConvert)
    {
        IPRange ipRange = getHitRangeCity(ip, needConvert);

        if (ipRange == null) {
            return null;
        }

        return ipRange.getProvinceCity();
    }
	
	//根据ip获取城市名
    public String getIPCityName(long ip, boolean needConvert)
    {
        String provinceCity = this.getIPProvinceCity(ip, needConvert);

        if (provinceCity == null) {
            return "未知";
        }

        return provinceCity;
    }

    public static void main(String args[]) throws Exception{
        IPDistrCity ipd = new IPDistrCity("mysql://hadoop:HA#2jsOw%x@192.168.71.45/db_ip_distribution_12_Q1?useUnicode=true&amp;characterEncoding=utf8");
        //System.out.println(ipd.getIPProvinceName(Long.valueOf(args[0]), true));
    }
}
