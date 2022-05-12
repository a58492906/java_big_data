package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.NamespaceDescriptor;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

/**
 * @author xjm
 * @version 1.0
 * @date 2022-05-12 15:34
 */
public class HbaseService {

    public static Connection createConnection() throws IOException {
        Configuration cfg = HBaseConfiguration.create();
         //在设置配置时，最好将ZooKeeper的三个节点都配置，否则性能会受影响（也可以写成其IP地址形式）
       cfg.set("hbase.zookeeper.quorum", "emr-worker-2，emr-worker-1，emr-header-");
       cfg.set("hbase.zookeeper.property.clientPort", "2181");
       // cfg.set("hbase.zookeeper.quorum", "localhost");
        return ConnectionFactory.createConnection(cfg);
    }



    public static void createTable(Connection connection,String myTableName,String[] colFamily) throws IOException{
        Admin admin = connection.getAdmin();
        TableName tableName=TableName.valueOf(myTableName);
        if (admin.tableExists(tableName)) {
            System.out.println("table is exist");
        }else {
            List<ColumnFamilyDescriptor> colFamilyList=new ArrayList<>();
            TableDescriptorBuilder tableDesBuilder=TableDescriptorBuilder.newBuilder(tableName);
            for(String str:colFamily) {
                ColumnFamilyDescriptor colFamilyDes=ColumnFamilyDescriptorBuilder.newBuilder(str.getBytes()).build();
                colFamilyList.add(colFamilyDes);
            }
            TableDescriptor tableDes=tableDesBuilder.setColumnFamilies(colFamilyList).build();
            admin.createTable(tableDes);
        }
    }


    public static void createNamespace(Connection connection, String tablespace) throws IOException {

        Admin admin = connection.getAdmin();
        admin.createNamespace(NamespaceDescriptor.create(tablespace).build());
        System.out.println("成功创建表空间 " + tablespace);
    }

    public static void insert(Connection connection, String tableName, String rowKey, String columnFamily, String column,
                              String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowKey.getBytes());
        put.addColumn(columnFamily.getBytes(), column.getBytes(), value.getBytes());
        table.put(put);
    }

    public static void scan(Connection connection, String tableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        Result tmp;
        while ((tmp = scanner.next()) != null) {
            List<Cell> cells = tmp.listCells();
            for (Cell cell : cells) {
                String rk = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                String cf = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                String column = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println(rk + "column:" + cf + ":" + column + ",value=" + value);
            }
        }
    }


    public static boolean deleteRow(Connection connection, String tableName, String rowkey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        table.delete(delete);
        System.out.println("成功删除 " + tableName + " " + rowkey);
        return true;
    }

    public static void deleteTable(Connection connection, String tableName) throws IOException {
        Admin admin = connection.getAdmin();
        if (admin.isTableDisabled(TableName.valueOf(tableName))) {
            System.out.println("table不存在");
            return;
        }
        if (!admin.isTableDisabled(TableName.valueOf(tableName))) {
            admin.disableTable(TableName.valueOf(tableName));
        }
        admin.deleteTable(TableName.valueOf(tableName));
        System.out.println("成功删除表 " + tableName);
    }

}



