/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpledb.query;

/**
 *
 * @author teo
 */
public class BoolConstant implements Constant {

    private Boolean val;

    /**
     * Create a constant by wrapping the specified boolean.
     *
     * @param n the boolean value
     */
    public BoolConstant(boolean n) {
        val = new Boolean(n);
    }

    /**
     * Unwraps the Boolean and returns it.
     *
     * @see simpledb.query.Constant#asJavaVal()
     */
    public Object asJavaVal() {
        return val;
    }

    public boolean equals(Object obj) {
        BoolConstant bc = (BoolConstant) obj;
        return bc != null && val.equals(bc.val);
    }

    public int compareTo(Constant c) {
        BoolConstant bc = (BoolConstant) c;
        return val.compareTo(bc.val);
    }

    public int hashCode() {
        return val.hashCode();
    }

    public String toString() {
        return val.toString();
    }
}
