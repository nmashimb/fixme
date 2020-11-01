package com.fixme.models;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

public class BrokerServer
{
    private int uniqueID = 100_000;
    private ByteBuffer bb = null;
    private String del = "" + (char)1;

    public BrokerServer() throws IOException {
        int[] ports = {5000,5001};
        Selector selector = Selector.open();
        final Map<String, SocketChannel> marketChannels = new HashMap<String, SocketChannel>();
        final Map<String, SocketChannel> brokerChannels = new HashMap<String, SocketChannel>();

        for (int port : ports) {
            ServerSocketChannel serverChannel = ServerSocketChannel.open();
            serverChannel.configureBlocking(false);
            serverChannel.socket().bind(new InetSocketAddress(port));
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);
            System.out.println("Listening to port "+port);
        }
    
        while (true) {
            selector.select();
            Iterator<SelectionKey> selectedKeys = selector.selectedKeys().iterator();
            while (selectedKeys.hasNext()) {
                SelectionKey selectedKey = selectedKeys.next();
                selectedKeys.remove();
                if (selectedKey.isAcceptable()) {
                    SocketChannel sc = ((ServerSocketChannel) selectedKey.channel()).accept();
                    String id;
                    ByteBuffer bb = null;
                    sc.configureBlocking(false);
                    switch (sc.socket().getLocalPort()) {
                        case 5000:
                            sc.register(selector, SelectionKey.OP_READ);
                            id = getNewID()+"";
                            bb = ByteBuffer.wrap(id.getBytes());
                            sc.write(bb);
                            brokerChannels.put(id, sc);
                            System.out.println("Connection Accepted: " + sc.getLocalAddress());
                            break;
                        case 5001:
                            sc.register(selector, SelectionKey.OP_READ);
                            id = getNewID()+"";
                            bb = ByteBuffer.wrap(id.getBytes());
                            sc.write(bb);
                            marketChannels.put(id, sc);
                            System.out.println("Connection Accepted: " + sc.getLocalAddress());
                            break;
                    }
                }
                else if (selectedKey.isReadable()) {
                    String result;
                    SocketChannel sc = (SocketChannel) selectedKey.channel();
                    switch (sc.socket().getLocalPort()) {
                        case 5000:
                            sc = (SocketChannel) selectedKey.channel();
                            bb = ByteBuffer.allocate(1024);
                            sc.read(bb);
                            result = new String(bb.array()).trim();
                            if (removeClient(result)){
                                String key = result.split(del)[0];
                                brokerChannels.remove(key);
                                sc.close();
                            }
                            else{
                                SocketChannel desChannel = destinationSocketChannel(marketChannels, result);
                                if (desChannel != null){
                                    bb = ByteBuffer.wrap(result.getBytes());
                                    desChannel.write(bb);
                                    System.out.println("Broker message: " + result);
                                }
                                else{
                                    String errorMessage = "id"+del+"8=FIX.4.4"+del+"35=D"+del+"55=ERROR!! TRY AGAIN"+del;
                                    bb = ByteBuffer.wrap(errorMessage.getBytes());
                                    sc.write(bb);
                                }
                            }
                            break;
                        case 5001:
                            sc = (SocketChannel) selectedKey.channel();
                            bb = ByteBuffer.allocate(1024);
                            sc.read(bb);
                            result = new String(bb.array()).trim();
                            if (removeClient(result)){
                                String key = result.split(del)[0];
                                marketChannels.remove(key);
                                sc.close();
                            }
                            else{
                                SocketChannel desChannel = destinationSocketChannel(brokerChannels, result);
                                if (desChannel != null){
                                    bb = ByteBuffer.wrap(result.getBytes());
                                    desChannel.write(bb);
                                }
                                else{
                                    String errorMessage = "id"+del+"8=FIX.4.4"+del+"35=D"+del+"55=ERROR!!"+del;
                                    bb = ByteBuffer.wrap(errorMessage.getBytes());
                                    sc.write(bb);
                                }
                            }
                            break;
                    }
                }

            }
        }
    }

    //Get methods
    public int getNewID(){
        uniqueID++;
        return (uniqueID);
    }

    public boolean removeClient(String result){
        String q = result.split(del)[1];
        if (q.trim().toLowerCase().matches("q")){
            return true;
        }
        else
            return false;
    }

    public static long getCRC32Checksum(byte[] bytes) {
	    Checksum crc32 = new CRC32();
	    crc32.update(bytes, 0, bytes.length);
	    return crc32.getValue();
    }

    private SocketChannel destinationSocketChannel(Map<String, SocketChannel> marketChannels, String msg){
        String[] msgArr = msg.split(del);
        String marketID = msgArr[5].split("=")[1];
        String checksum = msgArr[6].split("=")[1];
        String checksumTwo = "";

        for (int i = 0; i < msgArr.length - 1;i++){
            checksumTwo = checksumTwo + msgArr[i]+del;
        }
        checksumTwo = ""+getCRC32Checksum(checksumTwo.getBytes());
        //convert checksum
        if (marketChannels.get(marketID) != null && checksum.matches(checksumTwo))
            return (marketChannels.get(marketID));
        else
            return null;
    }
}