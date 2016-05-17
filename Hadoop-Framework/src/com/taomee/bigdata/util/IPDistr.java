package com.taomee.bigdata.util;

import java.util.*;
import java.sql.*;

public class IPDistr
{
    private class IPRange implements Comparable<IPRange>
    {
        private long startIP;
        private long endIP;
        private int provinceCode;

        public long getStartIP()
        {
            return this.startIP;
        }

        public long getEndIP()
        {
            return this.endIP;
        }

        public int getProvinceCode()
        {
            return this.provinceCode;
        }

        public IPRange(long startIP, long endIP, int provinceCode)
        {
            this.startIP = startIP;
            this.endIP = endIP;
            this.provinceCode = provinceCode;
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
    private IPRange[] ipRangeList = null;
    private IPRange[] ipCountryRangeList = null;
    private HashMap<Integer, String> provinceNameMap = new HashMap<Integer, String>();

    public IPDistr(Connection mysqlConn) throws SQLException
    {
        this.mysqlConn = mysqlConn;
        init(this.mysqlConn);
    }

    private void init(Connection mysqlConn) throws SQLException
    {
        Statement statement = mysqlConn.createStatement();
        ResultSet result = null;

        ArrayList<IPRange> list = new ArrayList<IPRange>();

        result = statement.executeQuery(
                "SELECT start_ip, end_ip, province_code FROM t_province_ip");
        while (result.next()) {
            long startIP = result.getLong(1);
            long endIP = result.getLong(2);
            int provinceCode = result.getInt(3);

            list.add(new IPRange(startIP, endIP, provinceCode));
        }

        ipRangeList = new IPRange[list.size()];
        list.toArray(ipRangeList);
        Arrays.sort(ipRangeList, new IPRangeComparator());

        result = statement.executeQuery(
                "SELECT province_code, province_name FROM t_province_code");
        while (result.next()) {
            int provinceCode = result.getInt(1);
            String provinceName = result.getString(2);
            provinceNameMap.put(provinceCode, provinceName);
        }

    }

    /**
     * @brief construct function, receive one mysql schema uri
     * @param  mysqluri ::= mysql://user:password@<host/ip>[:port]/database[?options]
     */
    public IPDistr(String mysqluri) throws ClassNotFoundException, SQLException
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

    public IPRange getHitRange(long ip, boolean needConvert)
    {
        int low = 0;
        int high = ipRangeList.length - 1;
        int mid = 0;
        long ipHost = needConvert ? this.convertToIPHost(ip) : ip;

        while (low <= high) {
            mid = (low + high) / 2;
            if (ipHost >= ipRangeList[mid].getStartIP()) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }

        if (high < 0 || high >= ipRangeList.length) {
            return null;
        }

        IPRange hit = ipRangeList[high];
        if (ipHost < hit.getStartIP() || ipHost > hit.getEndIP()) {
            return null;
        }

        return hit;
    }

    public int getIPProvinceCode(long ip, boolean needConvert)
    {
        IPRange ipRange = getHitRange(ip, needConvert);

        if (ipRange == null) {
            return 0;
        }

        return ipRange.getProvinceCode();
    }

    public int getIPProvinceCode(String ipStr, boolean needConvert)
    {
        long ip = 0;
        try { 
            ip = Long.valueOf(ipStr);
        } catch (NumberFormatException e) {
            e.printStackTrace();
            return 0;
        } 
        
        IPRange ipRange = getHitRange(ip, needConvert);
        if (ipRange == null) {
            return 0;
        }

        return ipRange.getProvinceCode();
    }

    public String getIPProvinceName(long ip, boolean needConvert)
    {
        int provinceCode = this.getIPProvinceCode(ip, needConvert);

        if (provinceCode == 0) {
            return "未知";
        }

        String provinceName = this.provinceNameMap.get(provinceCode);

        if (provinceName == null) {
            return "未知";
        }

        return provinceName;
    }

    public String getIPProvinceNameByCode(int provinceCode)
    {
        String provinceName = this.provinceNameMap.get(provinceCode);

        if (provinceName == null) {
            return "未知";
        }

        return provinceName;
    }

    public static void main(String args[]) throws Exception{
        IPDistr ipd = new IPDistr("mysql://hadoop:HA#2jsOw%x@192.168.71.45/db_ip_distribution_12_Q1?useUnicode=true&amp;characterEncoding=utf8");
        System.out.println(ipd.getIPProvinceName(Long.valueOf(args[0]), true));
    }
}
