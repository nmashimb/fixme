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
import java.io.FileWriter;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

public class Market
{
    private static BufferedReader input = null;
    private static int myID = 0;
    private static String msg = "initial";
    private static String del = "" + (char)1;
    private static File filename;
    private static final String fixv = "8=FIX.4.4";
    private static FileWriter myWriter;
    
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
            System.out.println("connected to port 5001?my ID: "+result);
        }
        if (key.isReadable()) {
            SocketChannel sc = (SocketChannel) key.channel();
            ByteBuffer bb = ByteBuffer.allocate(1024);
            sc.read(bb);
            String result = new String(bb.array()).trim();
            String msg = tradeMessage(result);
            bb = ByteBuffer.wrap(msg.getBytes());
            sc.write(bb);
        }
        if (key.isWritable()) {
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
            else
                printText();
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
                System.out.println("Market ID :"+getID()+"\nInstrument  Quantity\n");
                while (readText.hasNextLine()) {
                    count++;
                    System.out.println(readText.nextLine());
                }
            }
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    public static String tradeMessage(String result){
        String []reqArray = result.split(del);
        String message = reqArray[3].trim().toLowerCase();
        String brokerID = reqArray[0].trim().split("=")[1];
        String instrument = message.split("-")[0].split("=")[1].trim().toLowerCase();
        int quantity = Integer.parseInt(message.split("-")[1]);
        String buyOrSell = message.split("-")[3];

        if (buyOrSell.matches("buy")){
            try{
                filename = new File("instruments.txt");
                Scanner readText = new Scanner(filename);
                String instrumentsList = "";
                while (readText.hasNextLine()) {
                    instrumentsList = instrumentsList + readText.nextLine()+"\n";
                }
                readText.close();
                String[] instruments = instrumentsList.split("\n");
                for (int i = 0; i < instruments.length;i++){
                    String instTxt = instruments[i].split(" ")[0].trim().toLowerCase();
                    String qntyTxt = instruments[i].split(" ")[1].trim().toLowerCase();
                    if (instrument.matches(instTxt)){
                        int qntyTxtInt = Integer.parseInt(qntyTxt);
                        if (qntyTxtInt > 0 && (qntyTxtInt - quantity) > 0){
                            int dif = qntyTxtInt - quantity;
                            instruments[i] = instTxt+" "+dif;
                            myWriter = new FileWriter(filename); 
                            for (int j = 0; j < instruments.length;j++){
                                myWriter.write(instruments[j]+"\n");
                            }
                            myWriter.close();
                            return responseMessage("executed", brokerID);
                            
                            
                        }
                        else{
                            return responseMessage("rejected", brokerID);
                            
                        }
                    }
                }
            }
            catch (IOException ea){
                ea.printStackTrace();
            }
        }
        else if (buyOrSell.matches("sell")){
            try{
                filename = new File("instruments.txt");
                Scanner readText = new Scanner(filename);
                String instrumentsList = "";
                if (!readText.hasNextLine()){
                    myWriter = new FileWriter(filename); 
                    myWriter.write(instrument+" "+quantity);
                    myWriter.close();
                    readText.close();
                    return responseMessage("executed", brokerID);
                }
                while (readText.hasNextLine()) {
                    instrumentsList = instrumentsList + readText.nextLine()+"\n";
                }
                readText.close();
                String[] instruments = instrumentsList.split("\n");
                for (int i = 0; i < instruments.length;i++){
                    String instTxt = instruments[i].split(" ")[0].trim().toLowerCase();
                    String qntyTxt = instruments[i].split(" ")[1].trim().toLowerCase();
                    if (instrument.matches(instTxt)){
                        instruments[i] = instTxt+" "+(quantity + Integer.parseInt(instruments[i].split(" ")[1].trim()));
                        myWriter = new FileWriter(filename); 
                        for (int j = 0; j < instruments.length;j++){
                            myWriter.write(instruments[j]+"\n");
                        }
                        myWriter.close();
                        return responseMessage("executed", brokerID);
                    }
                }
                myWriter = new FileWriter(filename, true); 
                myWriter.write(instrument+" "+quantity);
                myWriter.close();
                return responseMessage("executed", brokerID);
            }
            catch (IOException ea){
                ea.printStackTrace();
            }
        }
        return responseMessage("rejected", brokerID);
    }

    public static long getCRC32Checksum(byte[] bytes) {
	    Checksum crc32 = new CRC32();
	    crc32.update(bytes, 0, bytes.length);
	    return crc32.getValue();
    }

    public static String responseMessage(String message, String receiverID){
        String msg = "id="+getID()+del+fixv+del+"35=D"+del+"55="+message+del+"49="+getID()+del+"56="+receiverID+del;;
        long checkSum = getCRC32Checksum(msg.getBytes());
        msg = msg + "10="+checkSum+del;
        return msg;
    }
}