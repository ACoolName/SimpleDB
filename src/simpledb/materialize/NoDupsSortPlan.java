package simpledb.materialize;

import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;

import java.util.*;

/**
 * The Plan class for the <i>sort</i> operator.
 *
 * @author Edward Sciore
 */
public class NoDupsSortPlan implements Plan {

    private Plan p;
    private Transaction tx;
    private Schema sch;
    private RecordComparator comp;

    /**
     * Creates a sort plan for the specified query.
     *
     * @param p the plan for the underlying query
     * @param sortfields the fields to sort by
     * @param tx the calling transaction
     */
    public NoDupsSortPlan(Plan p, List<String> sortfields, Transaction tx) {
        this.p = p;
        this.tx = tx;
        sch = p.schema();
        comp = new RecordComparator(sortfields);
    }

    /**
     * This method is where most of the action is. Up to 2 sorted temporary
     * tables are created, and are passed into SortScan for final merging.
     *
     * @see simpledb.query.Plan#open()
     */
    public Scan open() {
        Scan src = p.open();
        List<TempTable> runs = splitIntoRuns(src);
        src.close();
        while (runs.size() > 2) {
            runs = doAMergeIteration(runs);
        }
        return new SortScan(runs, comp);
    }

    /**
     * Returns the number of blocks in the sorted table, which is the same as it
     * would be in a materialized table. It does <i>not</i> include the one-time
     * cost of materializing and sorting the records.
     *
     * @see simpledb.query.Plan#blocksAccessed()
     */
    public int blocksAccessed() {
        // does not include the one-time cost of sorting
        Plan mp = new MaterializePlan(p, tx); // not opened; just for analysis
        return mp.blocksAccessed();
    }

    /**
     * Returns the number of records in the sorted table, which is the same as
     * in the underlying query.
     *
     * @see simpledb.query.Plan#recordsOutput()
     */
    public int recordsOutput() {
        return p.recordsOutput();
    }

    /**
     * Returns the number of distinct field values in the sorted table, which is
     * the same as in the underlying query.
     *
     * @see simpledb.query.Plan#distinctValues(java.lang.String)
     */
    public int distinctValues(String fldname) {
        return p.distinctValues(fldname);
    }

    /**
     * Returns the schema of the sorted table, which is the same as in the
     * underlying query.
     *
     * @see simpledb.query.Plan#schema()
     */
    public Schema schema() {
        return sch;
    }

    private List<TempTable> splitIntoRuns(Scan src) {
        List<TempTable> temps = new ArrayList<TempTable>();
        src.beforeFirst();
        if (!src.next()) {
            return temps;
        }
        TempTable currenttemp = new TempTable(sch, tx);
        temps.add(currenttemp);
        UpdateScan currentscan = currenttemp.open();
        while (copy(src, currentscan)) {
//            System.out.println("while " + src.getString("name") + " " + currentscan.getString("name"));
            if (comp.compare(src, currentscan) < 0) {
                // start a new run
//                System.out.println("SHIT SHIT SHITSTORM");
                currentscan.close();
                currenttemp = new TempTable(sch, tx);
                temps.add(currenttemp);
                currentscan = (UpdateScan) currenttemp.open();
                
//            ///REEEEALLY NOT SURE IF THIS SH!T WILL WORK    
//            } else if (comp.compare(src, currentscan) == 0) {
//                //start a new run
//                currentscan.close();
//                currentscan = (UpdateScan) currenttemp.open();
            }
        }
//        System.out.println(temps.toString());
        currentscan.close();
        return temps;
    }

    private List<TempTable> doAMergeIteration(List<TempTable> runs) {
        List<TempTable> result = new ArrayList<TempTable>();
        while (runs.size() > 1) {
            TempTable p1 = runs.remove(0);
            TempTable p2 = runs.remove(0);
            result.add(mergeTwoRuns(p1, p2));
        }
        if (runs.size() == 1) {
            result.add(runs.get(0));
        }
        return result;
    }

    private TempTable mergeTwoRuns(TempTable p1, TempTable p2) {
        Scan src1 = p1.open();
        Scan src2 = p2.open();
        TempTable result = new TempTable(sch, tx);
        UpdateScan dest = result.open();

        boolean hasmore1 = src1.next();
        boolean hasmore2 = src2.next();
        while (hasmore1 && hasmore2) {
            System.out.println("merge " + src1.getString("name") + " " + src2.getString("name") + " " + comp.compare(src1, src2));
            if (comp.compare(src1, src2) == 0) {
//                System.out.println("I'M ALIVE!");
                hasmore1 = copy(src1, dest);
                hasmore2 = src2.next();
            } else if (comp.compare(src1, src2) < 0) {
                hasmore1 = copy(src1, dest);
                // if the photosynthesises are the same don't include them in the result
            } else {
                hasmore2 = copy(src2, dest);
            }
//            System.out.println("Hasmore1 " + hasmore1 + " hasmore2 " + hasmore2);
        }
        
        Scan temp;
        if (hasmore1) {
            while (hasmore1) {
                System.out.println("hasmore1 " + src1.getString("name"));
                temp = src1;
                hasmore1 = copy(src1, dest);
                while (src1 == temp && hasmore1) {
                    System.out.println("hasmore1 while " + src1.getString("name"));
                    hasmore1 = src1.next();
                }
            }
        } else {
            while (hasmore2) {
                System.out.println("hasmore2 " + src2.getString("name"));
                temp = src2;
                hasmore2 = copy(src2, dest);
                while (src2 == temp && hasmore2) {
                    System.out.println("hasmore2 while " + src2.getString("name"));
                    hasmore2 = src2.next();
                }
            }
        }
        src1.close();
        src2.close();
        dest.close();
        return result;
    }

    private boolean copy(Scan src, UpdateScan dest) {
        dest.insert();
        for (String fldname : sch.fields()) {
            dest.setVal(fldname, src.getVal(fldname));
        }
        return src.next();
    }
}
