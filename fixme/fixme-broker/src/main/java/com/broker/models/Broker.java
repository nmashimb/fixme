package com.broker.models;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

public class Broker
{
    private static BufferedReader input = null;
    private static int myID;
    private static final String fixv = "8=FIX.4.4";
    private static String del = "" + (char)1;

    public Broker() throws Exception
    {
        InetSocketAddress addr = new InetSocketAddress(InetAddress.getByName("localhost"), 5000);
        Selector selector = Selector.open();
        SocketChannel sc = SocketChannel.open();
        sc.configureBlocking(false);
        sc.connect(addr);
        sc.register(selector, SelectionKey.OP_CONNECT | SelectionKey.OP_READ | SelectionKey.OP_WRITE);
        input = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            if (selector.select() > 0) {
                Boolean doneStatus = processReadySet(selector.selectedKeys());
                if (doneStatus) {
                    break;
                }
            }
        }
        sc.close();
    }

    public static Boolean processReadySet(Set readySet) throws Exception 
    {
        SelectionKey key = null;
        Iterator iterator = null;
        iterator = readySet.iterator();
        while (iterator.hasNext()) {
            key = (SelectionKey) iterator.next();
            iterator.remove();
        }
        if (key.isConnectable()) {
            Boolean connected = processConnect(key);
            if (!connected) {
                return true;
            }
            SocketChannel sc = (SocketChannel) key.channel();
            ByteBuffer bb = ByteBuffer.allocate(1024);
            sc.read(bb);
            String result = new String(bb.array()).trim();
            setID(Integer.parseInt(result));
            System.out.println("connected to port 5000?id: "+result);
        }
        if (key.isReadable()) {
            SocketChannel sc = (SocketChannel) key.channel();
            ByteBuffer bb = ByteBuffer.allocate(1024);
            sc.read(bb);
            String result = new String(bb.array()).trim();
            //WHAT DO WE DO WITH MESSAGE

            System.out.println("Message: " + result);
        }
        if (key.isWritable()) {
            System.out.println("Type a message (type r to refresh, q to stop): \n1.Buy\n2.Sell");
            String msg = input.readLine();
            SocketChannel sc = (SocketChannel) key.channel();
            
            if (msg.matches("q")){
                msg = getID()+del+msg;
                ByteBuffer bb = ByteBuffer.wrap(msg.getBytes());
                sc.write(bb);
                return true;
            }
            else if (!msg.trim().toLowerCase().matches("r")){
                if (msg.trim().toLowerCase().matches("1")) {
                    
                    System.out.println("Enter Instrument name: ");
                    String instrument = input.readLine().trim().toLowerCase();;
                    System.out.println("Enter Instrument quantity: ");
                    String quantity = input.readLine().trim().toLowerCase();;
                    instrument = instrument+"-"+quantity;
                    System.out.println("Enter market ID: ");
                    String marketID = input.readLine().trim().toLowerCase();
                    System.out.println("Enter price: ");
                    String price = input.readLine().trim().toLowerCase();
                    instrument = instrument+"-"+price;

                    msg = buyMessage(instrument, marketID);
                    ByteBuffer bb = ByteBuffer.wrap(msg.getBytes());
                    sc.write(bb);
                }
                else if (msg.trim().toLowerCase().matches("2")) {
                    
                }
                ByteBuffer bb = ByteBuffer.wrap(msg.getBytes());
                sc.write(bb);
            }
        }
        return false;
    }

    public static Boolean processConnect(SelectionKey key) 
    {
        SocketChannel sc = (SocketChannel) key.channel();
        try {
            while (sc.isConnectionPending()) {
                sc.finishConnect();
            }
        } catch (IOException e) {
            key.cancel();
            e.printStackTrace();
            return false;
        }
        return true;
    }

    //Setters
    public static void setID(int id)
    {
        myID = id;
    }

    //Getters
    public static int getID()
    {
        return myID;
    }

    public static long getCRC32Checksum(byte[] bytes) {
	    Checksum crc32 = new CRC32();
	    crc32.update(bytes, 0, bytes.length);
	    return crc32.getValue();
    }
    
    public static String buyMessage(String instrument, String receiverID){
        String msg = "id="+getID()+del+fixv+del+"35=D"+del+"55="+instrument+del+"49="+getID()+del+"56="+receiverID+del;;
        long checkSum = getCRC32Checksum(msg.getBytes());
        msg = msg + "10="+checkSum+del;
        return msg;
        /*if (cash > 0)
            return msg;
        else
            return "bye";*/
    }

    /*public static String sellMessage(int dst){
        String del = "|";
        String msg = "id="+getID()+del+fixv+del+"35=D"+del+"54=2"+del+"38=2"+del+"44=55"+del+"55=WTCSOCKS"+del;
        msg += "50="+getID()+del+"49="+getID()+del+"56="+dst+del;
        return msg;
    }*/
}