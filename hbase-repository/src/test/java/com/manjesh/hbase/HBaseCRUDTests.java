package com.manjesh.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import javax.swing.*;

/**
 * Author: mg153v (Manjesh Gowda). Creation Date: 3/27/2017.
 */
public class HBaseCRUDTests {

    static String tableName = "emp";
    static String familyName = "personal data";

    @Test
    public void testHBasePUT() throws Exception {
        Configuration config = HBaseConfiguration.create();
        //config.set("hbase.zookeeper.quorum", "192.168.99.101");
        config.set("hbase.zookeeper.quorum", "hbase-vm");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        try (HTable htable = new HTable(config, tableName)) {
            int total = 100;
            long t1 = System.currentTimeMillis();
            for (int i = 0; i < total; i++) {
                int userid = i;
                String email = "user-" + i + "@foo.com";
                String phone = "555-1234";

                byte[] key = Bytes.toBytes(userid);
                Put put = new Put(key);
                put.add(Bytes.toBytes(familyName), Bytes.toBytes("city"), Bytes.toBytes(email));  // <-- email goes here
                put.add(Bytes.toBytes(familyName), Bytes.toBytes("name"), Bytes.toBytes(phone));  // <-- phone goes here
                htable.put(put);

            }
            long t2 = System.currentTimeMillis();
            System.out.println("inserted " + total + " users  in " + (t2 - t1) + " ms");

        }
    }

    @Test
    public void testHBaseGET() throws Exception {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "hbase-vm");
        config.set("hbase.zookeeper.property.clientPort", "2181");

        try (HTable htable = new HTable(config, tableName)) {
            int total = 100;
            long t1 = System.currentTimeMillis();

            Get get = new Get(Bytes.toBytes("1"));

            final Result result = htable.get(get);
            final Set<Map.Entry<byte[], byte[]>> entries =
                    result.getFamilyMap(Bytes.toBytes("personal data")).entrySet();
            for (Map.Entry<byte[], byte[]> entry : entries) {
                System.out.println(Bytes.toString(entry.getKey()) + " **** " + Bytes.toString(entry.getValue()));
            }

            System.out.println(result.toString());
            System.out.println(Bytes.toString(
                    result.getValue(Bytes.toBytes("personal data"), Bytes.toBytes("city"))));

            long t2 = System.currentTimeMillis();
            System.out.println("inserted " + total + " users  in " + (t2 - t1) + " ms");

        }
    }


    @Test
    public void testHBaseUPDATE() throws Exception {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "hbase-vm");
        config.set("hbase.zookeeper.property.clientPort", "2181");

        try (HTable htable = new HTable(config, tableName)) {
            byte[] key = Bytes.toBytes("1");
            Put put = new Put(key);
            put.add(Bytes.toBytes(familyName), Bytes.toBytes("city"), Bytes.toBytes("Kailash"));  // <-- email goes here
            htable.put(put);

        }
    }
}
