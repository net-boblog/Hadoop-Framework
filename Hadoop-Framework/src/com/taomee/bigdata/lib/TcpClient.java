package com.taomee.bigdata.lib;

import java.net.Socket;
import java.net.UnknownHostException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.io.InputStream;
import java.io.OutputStream;
import com.taomee.bigdata.lib.TcpClient;

public class TcpClient {

    private Socket socket = null;
	private OutputStream os;
	private InputStream is;
    private static Logger log = Logger.getInstance();
    private String server = null;
    private String port = null;

    public boolean connect(String server, String port) {
		try {
			socket = new Socket(server, Integer.valueOf(port));
            os = socket.getOutputStream();
            is = socket.getInputStream();
		} catch (UnknownHostException e) {
			log.ERROR_LOG("UnknownHost: " 
						+ server + ":"
						+ port);
			return false;
		} catch (IOException e) {
			log.ERROR_LOG("Connnect to Server Faild: " 
					+ server + ":"
					+ port);
            log.EXCEPTION_LOG(e);
			return false;
		}
        log.DEBUG_LOG("Connect to Server "
                + server + ":"
                + port);
        this.server = server;
        this.port = port;
		return true;
    }

	public static byte[] toLH(short n) {
		byte[] b = new byte[2];
		b[0] = (byte) (n & 0xff);
		b[1] = (byte) (n >> 8 & 0xff);
		return b;
	}
	
	public static byte[] toLH(int n) {
	    byte[] b = new byte[4];
	    b[0] = (byte) (n & 0xff);
	    b[1] = (byte) (n >> 8 & 0xff);
	    b[2] = (byte) (n >> 16 & 0xff);
	    b[3] = (byte) (n >> 24 & 0xff);
	    return b;
	}
	
	public static byte[] toLH(double d) {
		byte[] b = new byte[8];
		long l = Double.doubleToLongBits(d);
		b[0] = (byte) (l & 0xff);
	    b[1] = (byte) (l >> 8 & 0xff);
	    b[2] = (byte) (l >> 16 & 0xff);
	    b[3] = (byte) (l >> 24 & 0xff);
	    b[4] = (byte) (l >> 32 & 0xff);
	    b[5] = (byte) (l >> 40 & 0xff);
	    b[6] = (byte) (l >> 48 & 0xff);
	    b[7] = (byte) (l >> 56 & 0xff);
		return b;
	}
    public static byte[] toHL(double dd) {
        byte[] b = new byte[8];
        long l = Double.doubleToLongBits(dd);
        b[7] = (byte) (l & 0xff);
        b[6] = (byte) (l >> 8 & 0xff);
        b[5] = (byte) (l >> 16 & 0xff);
        b[4] = (byte) (l >> 24 & 0xff);
        b[3] = (byte) (l >> 32 & 0xff);
        b[2] = (byte) (l >> 40 & 0xff);
        b[1] = (byte) (l >> 48 & 0xff);
        b[0] = (byte) (l >> 56 & 0xff);
        return b;
    }
	
	public static byte[] toLH(String s) {
        try {
            return s == null ? "".getBytes("UTF-8") : s.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            log.EXCEPTION_LOG(e);
        }
        return new byte[0];
	}

    public int send(byte[] b) {
        if(socket.isClosed()) {
            connect(server, port);
        }
        try {
            os.write(b);
        } catch (IOException e) {
            log.EXCEPTION_LOG(e);
            return -1;
        }
        return b.length;
    }

    public byte[] recv(int length) {
        byte[] b = new byte[length];
        if(socket.isClosed()) {
            connect(server, port);
        }
        try {
            is.read(b);
        } catch (IOException e) {
            log.EXCEPTION_LOG(e);
            return null;
        }
        return b;
    }

}
