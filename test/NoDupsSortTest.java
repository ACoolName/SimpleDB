/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import simpledb.materialize.NoDupsSortPlan;
import simpledb.materialize.SortPlan;
import simpledb.metadata.TableMgr;
import simpledb.query.Scan;
import simpledb.query.TablePlan;
import simpledb.record.RecordFile;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/**
 *
 * @author teo
 */
public class NoDupsSortTest {
    
    static Transaction tx;
    static Schema schema;
//    MyOperation op;
    static final String tableName = "mytable";
    static final String dbName = "mytestdb";
    
    public NoDupsSortTest() {
    }
    
    @BeforeClass
    public static void setupClass() {
        SimpleDB.init(dbName);
        tx = new Transaction();
        
        schema = new Schema();
        schema.addStringField("name", TableMgr.MAX_NAME);
        schema.addIntField("age");
        SimpleDB.mdMgr().createTable(tableName, schema, tx);
        TableInfo tableInfo = SimpleDB.mdMgr().getTableInfo(tableName, tx);
        RecordFile file = new RecordFile(tableInfo, tx);

        file.insert();
        file.setString("name", "Peter");
        file.setInt("age", 23);

        file.insert();
        file.setString("name", "Ellen");
        file.setInt("age", 22);

        file.insert();
        file.setString("name", "Peter");
        file.setInt("age", 23);
        
        file.insert();
        file.setString("name", "John");
        file.setInt("age", 23);
        
        file.insert();
        file.setString("name", "Peter");
        file.setInt("age", 23);
        
        file.insert();
        file.setString("name", "Peter");
        file.setInt("age", 23);
        
        file.insert();
        file.setString("name", "Peter");
        file.setInt("age", 23);
        
        file.insert();
        file.setString("name", "John");
        file.setInt("age", 23);

        file.insert();
        file.setString("name", "Ellen");
        file.setInt("age", 22);
        
        file.insert();
        file.setString("name", "Ellen");
        file.setInt("age", 22);

        tx.commit();
    }
    
    @AfterClass
    public static void tearDownClass() throws IOException {
        SimpleDB.dropDatabase(dbName);
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
        tx.commit();
    }

    @Test
    public void sortNoDupsTest() {
        List<String> sortfields = new ArrayList<>();
        sortfields.add("name");
        TablePlan p = new TablePlan(tableName, tx);
        NoDupsSortPlan sp = new NoDupsSortPlan(p, sortfields, tx);
        Scan sortScan = sp.open();
        sortScan.beforeFirst();
        sortScan.next();
        System.out.println(sortScan.getString("name"));
        sortScan.next();
        System.out.println(sortScan.getString("name"));
        sortScan.next();
        System.out.println(sortScan.getString("name"));
        sortScan.next();
        System.out.println(sortScan.getString("name"));
        sortScan.next();
        System.out.println(sortScan.getString("name"));
        sortScan.next();
        System.out.println(sortScan.getString("name"));
        sortScan.next();
        System.out.println(sortScan.getString("name"));
        sortScan.next();
        System.out.println(sortScan.getString("name"));
        sortScan.next();
        System.out.println(sortScan.getString("name"));
        sortScan.next();
        System.out.println(sortScan.getString("name"));
//        assertEquals("Ellen", sortScan.getString("name"));
//        sortScan.next();
//        assertEquals("Ellen", sortScan.getString("name"));
//        sortScan.next();
//        assertEquals("John", sortScan.getString("name"));
//        sortScan.next();
//        assertEquals("Peter", sortScan.getString("name"));
    }
    
//    @Test
//    public void floatTestSecondRow() {
//        TableInfo ti = new TableInfo(tableName, schema);
//        TableScan ts = new TableScan(ti, tx);
//        ts.beforeFirst();
//        ts.next();
//        ts.next();
//        assertEquals(123.123f, ts.getFloat("price"), 0);
//    }
}
