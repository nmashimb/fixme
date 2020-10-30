package com.market.models;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.zip.CheckedInputStream;
import java.util.zip.Adler32;
import java.io.File;
import java.io.IOException;
import java.util.Scanner;


public class Market
{
    private static BufferedReader input = null;
    private static int myID = 0;
    private static String msg = "initial";
    private static String del = "" + (char)1;
    private static File filename;
    
    public Market() throws Exception {
        InetSocketAddress addr = new InetSocketAddress(InetAddress.getByName("localhost"), 5001);
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

    public static Boolean processReadySet(Set readySet) throws Exception {
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
            System.out.println("connected to port 5001?id: "+result);
        }
        if (key.isReadable()) {
            SocketChannel sc = (SocketChannel) key.channel();
            ByteBuffer bb = ByteBuffer.allocate(1024);
            sc.read(bb);
            String result = new String(bb.array()).trim();
            System.out.println("Message received from MarketServer: " + result);
        }
        if (key.isWritable()) { //not really needed!!!!
            printText();
            System.out.print("(type any key to refresh, q to stop)");
            String msg = input.readLine();
            SocketChannel sc = (SocketChannel) key.channel();
            if (msg.matches("q")){
                msg = getID()+del+msg;
                ByteBuffer bb = ByteBuffer.wrap(msg.getBytes());
                sc.write(bb);
                return true;
            }
            else{
                printText();
                /* msg = getID()+del+msg;
                ByteBuffer bb = ByteBuffer.wrap(msg.getBytes());
                sc.write(bb);*/
            }
        }
        return false;
    }

    public static Boolean processConnect(SelectionKey key) {
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

    //Set methods
    public static void setID(int id){
        myID = id;
    }

    //get methods
    public static int getID(){
        return myID;
    }

    public static void printText(){
        try{
            filename = new File("instruments.txt");
            Scanner readText = new Scanner(filename);
            int count = 0;

            if (!readText.hasNextLine())
                System.out.println("Textfile empty!!");
            else {
                System.out.println("Instrument  Quantity\n");
                while (readText.hasNextLine()) {
                    count++;
                    System.out.println(readText.nextLine());
                }
                System.out.println("///////////////////////");
            }
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }
}