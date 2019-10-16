package com.szewec.service;

import com.szewec.config.HBaseConfig;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by fantastyJ on 2018/12/13 3:54 PM
 */
@Service
public class HBaseServiceImpl implements HBaseService {

    @Autowired
    private HBaseConfig con;
    /**
     * 根据tableName，family 创建表
     */
    @Override
    public void createTable(String tableName, String[] family) {
        Connection connection = null;
        if (StringUtils.isBlank(tableName) || family == null || family.length == 0) {
            return;
        }
        try {
            connection = ConnectionFactory.createConnection(con.configuration());
            Admin admin = connection.getAdmin();
            HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
            for (int i = 0; i < family.length; i++) {
                desc.addFamily(new HColumnDescriptor(family[i]));
            }
            if (admin.tableExists(TableName.valueOf(tableName))) {
                System.out.println("table Exists!");
                System.exit(0);
            } else {
                admin.createTable(desc);
                System.out.println("create table Success!");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    /**
     * 根据tableName删除表
     */
    @Override
    public void deleteTable(String tableName) {
        Connection connection = null;
        if (StringUtils.isBlank(tableName)) {
            return;
        }
        try {
            connection = ConnectionFactory.createConnection(con.configuration());
            Admin admin = connection.getAdmin();
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
            System.out.println(tableName + " is deleted!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据tableName，rowkey，family coluem 查询单个数据
     */
    public String getValue(String tableName, String rowKey, String familyName, String column) {
        Table table = null;
        Connection connection = null;
        String res = "";
        if (StringUtils.isBlank(tableName) || StringUtils.isBlank(familyName)
                || StringUtils.isBlank(rowKey) || StringUtils.isBlank(column)) {
            return null;
        }
        try {
            connection = ConnectionFactory.createConnection(con.configuration());
            table = connection.getTable(TableName.valueOf(tableName));
            Get g = new Get(rowKey.getBytes());
            g.addColumn(familyName.getBytes(), column.getBytes());
            Result result = table.get(g);
            List<Cell> ceList = result.listCells();
            if (ceList != null && ceList.size() > 0) {
                for (Cell cell : ceList) {
                    res = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return res;
    }

    /**
     * 根据起始行键和终止行键输出信息
     */
    public List<String> getValueByStartStopRowKey(String tableName, String familyName, String column, String startRow, String stopRow) {
        Table table = null;
        Connection connection = null;
        if (StringUtils.isBlank(tableName) || StringUtils.isBlank(familyName)
                || StringUtils.isBlank(startRow) || StringUtils.isBlank(stopRow)
                || StringUtils.isBlank(column)) {
            return null;
        }
        List<String> rs = new ArrayList<>();
        try {
            connection = ConnectionFactory.createConnection(con.configuration());
            table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(startRow));
            scan.setStopRow(Bytes.toBytes(stopRow));
            ResultScanner result = table.getScanner(scan);
            result.forEach(r -> {
                Map map = r.getFamilyMap(Bytes.toBytes(familyName));
                List<Cell> cells = r.listCells();
                cells.forEach(c -> rs.add(Bytes.toString(CellUtil.cloneRow(c)) + ":::" + Bytes.toString(CellUtil.cloneValue(c))));
            });
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return rs;
    }
    /**
     * 根据tableName，rowkey，family coluem 插入单个数据
     */
    @Override
    public void insertRow(String tableName, String rowKey,
                                   String familyName, String column, String value) {
        if (StringUtils.isBlank(tableName) || StringUtils.isBlank(rowKey)
                || StringUtils.isBlank(familyName) || StringUtils.isBlank(column)
                || StringUtils.isBlank(value)) {
            return;
        }
        try {
            Connection connection = ConnectionFactory.createConnection(con.configuration());
            Table table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(column),
                    Bytes.toBytes(value));
            table.put(put);
        }catch (Exception e){
            e.printStackTrace();
        }
        System.out.println("update table Success!");
    }
    /**
     * 根据tableName，rowkey，family coluem 删除单个数据
     */
    public void deleteColumn(String tableName, String rowKey,
                                    String familyName, String column){
        if (StringUtils.isBlank(tableName) || StringUtils.isBlank(rowKey)
                || StringUtils.isBlank(familyName) || StringUtils.isBlank(column)) {
            return;
        }
        try {
            Connection connection = ConnectionFactory.createConnection(con.configuration());

            Table table = connection.getTable(TableName.valueOf(tableName));
            Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));
            deleteColumn.addColumn(Bytes.toBytes(familyName),
                    Bytes.toBytes(column));
            table.delete(deleteColumn);
        }catch (Exception e){
            e.printStackTrace();
        }
        System.out.println(familyName + ":" + column + "is deleted!");
    }
    /**
     * 根据tableName，rowkey 删除整行数据
     */
    public void deleteAllColumn(String tableName, String rowKey){

        if (StringUtils.isBlank(tableName) || StringUtils.isBlank(rowKey)) {
            return;
        }
        try {
            Connection connection = ConnectionFactory.createConnection(con.configuration());

            Table table = connection.getTable(TableName.valueOf(tableName));
            Delete deleteAll = new Delete(Bytes.toBytes(rowKey));
            table.delete(deleteAll);
        }catch (Exception e){
            e.printStackTrace();
        }
        System.out.println("all columns are deleted!");
    }

}

