package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentValues;
import android.net.Uri;
import android.os.Message;

import org.w3c.dom.Node;

import java.util.HashMap;
import java.util.Map;

import static android.R.id.message;

/**
 * Created by anushree on 4/13/17.
 */

public class NodeMessage implements Comparable<NodeMessage>{

    String portID;
    String hashID;
    String predecessor;
    String successor;
    String msgType;
    String key;
    String value;
    String originator;

    public NodeMessage(){
        portID = null ;
        hashID = null;
        predecessor = null;
        successor = null;
        msgType = "DUMMY";
        key = "";
        value = "";
        originator = null;
    }

    public NodeMessage(String ID, String hashID, String predecessor, String successor, String type){
        this.portID = ID;
        this.msgType = type;
        this.hashID = hashID;
        this.predecessor = predecessor;
        this.successor = successor;
        this.key = "";
        this.value = "";
        this.originator = null;
    }

    public String formMessage(){

        String str = portID+"~#~"+hashID+"~#~"+predecessor+"~#~"+successor+"~#~"+msgType+"~#~"+key+"~#~"+value+"~#~"+originator;

        return str;
    }

    public void breakMessage(String str){

        String[] msg = str.split("~#~");

        this.portID = msg[0];
        this.hashID = msg[1];
        this.predecessor = msg[2];
        this.successor = msg[3];
        this.msgType = msg[4];
        this.key =  msg[5];
        this.value = msg[6];
        this.originator = msg[7];

    }

    @Override
    public int compareTo(NodeMessage node) {
        String hash = node.hashID;
        return this.hashID.compareTo(hash);
    }
}