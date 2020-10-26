package com.fixme.models;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.HashMap;
import java.util.Map;


public class BrokerServer
{
    private int uniqueID = 100_000;
    private ByteBuffer bb = null;

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
                            System.out.println("Connection Accepted: " + sc.getLocalAddress() + "\n"+brokerChannels.size());
                            break;
                        case 5001:
                            sc.register(selector, SelectionKey.OP_READ);
                            id = getNewID()+"";
                            bb = ByteBuffer.wrap(id.getBytes());
                            sc.write(bb);
                            marketChannels.put(id, sc);
                            System.out.println("Connection Accepted: " + sc.getLocalAddress() + "\n"+marketChannels.size());
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
                            System.out.println("BrokerServer received a msg: " + result);
                            if (result.matches("q")){
                                //do something
                            }
                            break;
                        case 5001:
                            sc = (SocketChannel) selectedKey.channel();
                            bb = ByteBuffer.allocate(1024);
                            sc.read(bb);
                            result = new String(bb.array()).trim();
                            System.out.println("BrokerServer received a msg: " + result);
                            if (result.matches("q")){
                                //do something
                            }
                            bb = ByteBuffer.wrap(result.getBytes());
                            brokerChannels.get("100001").write(bb);
                            break;
                    }
                }

            }
        }
    }

    public int getNewID(){
        uniqueID++;
        return (uniqueID);
    }
}