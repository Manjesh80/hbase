package com.manjesh.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

/**
 * Author: mg153v (Manjesh Gowda). Creation Date: 3/27/2017.
 */
public class HBaseCRUDTests {

    static String tableName = "user";
    static String familyName = "d";

    @Test
    public void testHBaseCRUD() throws Exception {

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "192.168.99.101");
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

                put.add(Bytes.toBytes(familyName), Bytes.toBytes("FIRST_NAME"), Bytes.toBytes(email));  // <-- email goes here
                put.add(Bytes.toBytes(familyName), Bytes.toBytes("LAST_NAME"), Bytes.toBytes(phone));  // <-- phone goes here
                htable.put(put);

            }
            long t2 = System.currentTimeMillis();
            System.out.println("inserted " + total + " users  in " + (t2 - t1) + " ms");

        }
    }
}
