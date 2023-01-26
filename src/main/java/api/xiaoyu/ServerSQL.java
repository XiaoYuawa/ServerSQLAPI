package api.xiaoyu;

import api.xiaoyu.Exceptions.InsertException;
import api.xiaoyu.Exceptions.QueryException;
import api.xiaoyu.Exceptions.UpdateException;
import cc.carm.lib.easysql.EasySQL;
import cc.carm.lib.easysql.api.SQLManager;
import cc.carm.lib.easysql.api.action.query.QueryAction;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public final class ServerSQL {

    private ServerSQL(){}

    private static final Class serverSQLClass;
    private static final SQLManager sqlManager;

    //初始化sqlManager
    static {
        Field f;
        Object obj;
        //获取SeverSQL的类
        try {
            serverSQLClass = Class.forName("plugin.xiaoyu.ServerSQL"); //获取类的字节码文件对象
        } catch (ClassNotFoundException e) {
            throw new RuntimeException();
        }
        //获取私有成员变量sqlManager，并对它进行赋值
        try {
            f = serverSQLClass.getDeclaredField("sqlManager");
            f.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
        try {
            obj = f.get(null);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        sqlManager = (SQLManager) obj;

    }

    /**
     * 向数据库插入数据
     *
     * @param table 目标表名
     * @param map 存放数据库的字段名和值 key为字段,value为值
     */
    public static void insert(String table, LinkedHashMap<String, Object> map) {
        String[] key = new String[map.size()];
        Object[] value = new Object[map.size()];
        int n = 0;
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            key[n] = entry.getKey();
            value[n] = entry.getValue();
            n += 1;
        }
        try {
            sqlManager.createInsert(table)
                    .setColumnNames(key)
                    .setParams(value)
                    .execute();
        } catch (SQLException e) {
            throw new InsertException("添加失败!检查数据库中是否存在该表或添加的数据的数量与字段是否一致");
        }
    }

    /**
     * 从数据库中查询数据
     *
     * @param table 目标表名
     * @param columns 查询的数据
     *
     * @return {@link Object} 二维数组 该数组中的每一个数组都是对应的值
     * <br>如: 这是一个表
     * <br>name | uuid | money
     * <br>test1 | 1 | 100
     * <br>test2 | 2 | 114514
     * <br>则返回的数据为 [ [test1,1,100], [test2,2,114514] ]
     */
    public static Object[][] query(String table, String... columns) {
        int length = Arrays.stream(columns).toArray().length;
        ArrayList<Object> arrayList = new ArrayList<>();
        //查询构造器
        QueryAction qa = sqlManager.createQuery()
                .inTable(table) //查询的表
                .selectColumns(columns) //查询的字段
                .build();
        //处理
        try {
            ResultSet rs = qa.execute().getResultSet();
            int n = 0;
            while (rs.next()) {
                Object[] l = new Object[length];
                for (int i = 0; i < length; i++) {
                    l[i] = rs.getObject(i + 1);
                }
                arrayList.add(l);
                n++;
            }
        } catch (SQLException e) {
            throw new QueryException("查询错误!请检查是否存在该表或字段");
        }
        Object[][] list = new Object[arrayList.size()][length]; //Object[]表示有多少组数据 Object[][]为数据的长度
        arrayList.forEach(l -> { //l表示有多少组数据
            for (int i = 0; i < arrayList.size(); i++) {
                list[i] = (Object[]) l; //赋值
            }
        });
        return list;
    }
    /**
     * 从数据库中查询数据
     *
     * @param table 目标表名
     * @param conditionName 条件名(直接填写即可,无需拼接)
     * @param condition 条件值
     * @param columns 查询的数据
     *
     * @return {@link Object} 二维数组 该数组中的每一个数组都是对应的值
     * <br>如: 这是一个表
     * <br>name | uuid | money
     * <br>test1 | 1 | 100
     * <br>test2 | 2 | 114514
     * <br>则返回的数据为 [ [test1,1,100], [test2,2,114514] ]
     */
    public static Object[][] queryCondition(String table, String conditionName, String condition,String... columns) {
        int length = Arrays.stream(columns).toArray().length;
        ArrayList<Object> arrayList = new ArrayList<>();
        //查询构造器
        QueryAction qa = sqlManager.createQuery()
                .inTable(table) //查询的表
                .selectColumns(columns) //查询的字段
                .addCondition(conditionName,condition)
                .build();
        //处理
        try {
            ResultSet rs = qa.execute().getResultSet();
            int n = 0;
            while (rs.next()) {
                Object[] l = new Object[length];
                for (int i = 0; i < length; i++) {
                    l[i] = rs.getObject(i + 1);
                }
                arrayList.add(l);
                n += 1;
            }
        } catch (SQLException e) {
            throw new QueryException("查询错误!请检查是否存在该表或字段");
        }
        Object[][] list = new Object[arrayList.size()][length];
        arrayList.forEach(l -> {
            for (int i = 0; i < arrayList.size(); i++) {
                list[i] = (Object[]) l;
            }
        });
        return list;
    }

    /**
     * 将数据库中的数据全部删除
     *
     * @param table 目标表名
     */
    public static void delete(String table) {
        sqlManager.createDelete(table)
                .build()
                .execute(null);
    }
    /**
     * 删除数据库中的指定数据
     *
     * @param table 目标表名
     * @param conditionName 条件名(直接填写即可,无需拼接)
     * @param condition 条件值
     */
    public static void delete(String table, String conditionName, String condition){
        sqlManager.createDelete(table)
                .addCondition(conditionName,condition)
                .build()
                .execute(null);
    }

    /**
     * 更新数据库中的所有数据
     *
     * @param table 目标表名
     * @param columnValues 字段名和值的键值对, key为字段名, value为值
     */
    public static void update(String table, LinkedHashMap<String, Object> columnValues) {
        try {
            sqlManager.createUpdate(table)
                    .setColumnValues(columnValues)
                    .build()
                    .execute();
        } catch (SQLException e) {
            throw new UpdateException("更新失败!检查数据库中是否存在该表或字段");
        }
    }
    /**
     * 根据条件更新数据库中的数据
     *
     * @param table 目标表名
     * @param conditionName 条件名(直接填写即可,无需拼接)
     * @param condition 条件值
     * @param columnValues 字段名和值的键值对, key为字段名, value为值
     */
    public static void update(String table, String conditionName, String condition ,LinkedHashMap<String, Object> columnValues) {
        try {
            sqlManager.createUpdate(table)
                    .setColumnValues(columnValues)
                    .addCondition(conditionName,condition)
                    .build()
                    .execute();
        } catch (SQLException e) {
            throw new UpdateException("更新失败!检查数据库中是否存在该表或字段或者条件是否正确书写");
        }
    }

    public static void createTable(String name, String columns){
        sqlManager.executeSQL("create table if not exists " + name + " ("+columns+");");
    }

}