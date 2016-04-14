package simpledb.query;

/**
 * The class that wraps Java strings as database constants.
 *
 * @author Edward Sciore
 */
public class FloatConstant implements Constant {

    private Float val;

    /**
     * Create a constant by wrapping the specified string.
     *
     * @param s the string value
     */
    public FloatConstant(Float s) {
        val = s;
    }

    /**
     * Unwraps the string and returns it.
     *
     * @see simpledb.query.Constant#asJavaVal()
     */
    public Float asJavaVal() {
        return val;
    }

    public boolean equals(Object obj) {
        FloatConstant fc = (FloatConstant) obj;
        return fc != null && val.equals(fc.val);
    }

    public int compareTo(Constant c) {
        FloatConstant fc = (FloatConstant) c;
        return val.compareTo(fc.val);
    }

    public int hashCode() {
        return val.hashCode();
    }

    public String toString() {
        return val.toString();
    }
}
