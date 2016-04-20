/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import java.io.IOException;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;
import simpledb.metadata.TableMgr;
import simpledb.query.TableScan;
import simpledb.record.RecordFile;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/**
 *
 * @author teo
 */
public class FloatTest {

    static Transaction tx;
    static Schema schema;
//    MyOperation op;
    static final String tableName = "mytable";
    static final String dbName = "mytestdb";

    @BeforeClass
    public static void setupClass() {
        SimpleDB.init(dbName);
        tx = new Transaction();
        
        schema = new Schema();
        schema.addStringField("name", TableMgr.MAX_NAME);
        schema.addIntField("age");
        schema.addFloatField("price");
        SimpleDB.mdMgr().createTable(tableName, schema, tx);
        TableInfo tableInfo = SimpleDB.mdMgr().getTableInfo(tableName, tx);
        RecordFile file = new RecordFile(tableInfo, tx);

        file.insert();
        file.setString("name", "Peter");
        file.setInt("age", 23);
        file.setFloat("price", 0.00f);

        file.insert();
        file.setString("name", "John");
        file.setInt("age", 23);
        file.setFloat("price", 123.123f);

        file.insert();
        file.setString("name", "Ellen");
        file.setInt("age", 25);
        file.setFloat("price", 9000.0000001f);

        tx.commit();
    }

    @AfterClass
    public static void tearDownClass() throws IOException {
        SimpleDB.dropDatabase(dbName);
    }

    @After
    public void tearDown() {
        tx.commit();
    }

    @Test
    public void floatTest() {
        TableInfo ti = new TableInfo(tableName, schema);
        TableScan ts = new TableScan(ti, tx);
        ts.beforeFirst();
        ts.next();
        assertEquals(0.00f, ts.getFloat("price"), 0);
    }
    
    @Test
    public void floatTestSecondRow() {
        TableInfo ti = new TableInfo(tableName, schema);
        TableScan ts = new TableScan(ti, tx);
        ts.beforeFirst();
        ts.next();
        ts.next();
        assertEquals(123.123f, ts.getFloat("price"), 0);
    }
}
