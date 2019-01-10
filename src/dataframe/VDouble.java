package dataframe;

public class VDouble extends Value implements Cloneable{
    public double val;
    public VDouble(double v){ val = v; }
    public VDouble(){
        val = 0;
    }

    @Override
    public String toString() {
        return Double.toString(val);
    }

    @Override
    public Value add(Value v) throws IncompatibleType {
        if(v instanceof VDouble){
            val = val+((VDouble) v).val;
            return this;
        }
        if(v instanceof VInt){
            val = val+ (double) ((VInt) v).val;
            return this;
        }
        if(v instanceof VFloat){
            val = val+ ((VFloat) v).val;
            return this;
        }
        throw new IncompatibleType();
    }

    @Override
    public Value sub(Value v) throws IncompatibleType {
        if(v instanceof VDouble){
            val = val-((VDouble) v).val;
            return this;
        }
        if(v instanceof VInt){
            val = val- (double) ((VInt) v).val;
            return this;
        }
        if(v instanceof VFloat){
            val = val- ((VFloat) v).val;
            return this;
        }
        throw new IncompatibleType();
    }

    @Override
    public Value div(Value v) throws DivisionByZero {
        if (v instanceof VInt) {
            if (((VInt) v).val == 0) throw new DivisionByZero();
            val = val / (double) ((VInt) v).val;
            return this;
        }
        if (v instanceof VDouble) {
            if (((VDouble) v).val == 0) throw new DivisionByZero();
            val = val / ((VDouble) v).val;
            return this;
        }
        if (v instanceof VFloat) {
            if (((VFloat) v).val == 0) throw new DivisionByZero();
            val = val / (double) ((VFloat) v).val;
            return this;
        }
        return null;
    }

    @Override
    public Value mul(Value v) throws IncompatibleType {
        if(v instanceof VDouble){
            val = val*((VDouble) v).val;
            return this;
        }
        if(v instanceof VInt){
            val = val* (double) ((VInt) v).val;
            return this;
        }
        if(v instanceof VFloat){
            val = val* ((VFloat) v).val;
            return this;
        }
        throw new IncompatibleType();
    }

    @Override
    public Value pow(Value v) throws IncompatibleType {
        if(v instanceof VDouble){
            val = Math.pow(val,((VDouble) v).val);
            return this;
        }
        if(v instanceof VInt){
            val = Math.pow(val,(double) ((VInt) v).val);
            return this;
        }
        if(v instanceof VFloat){
            val = Math.pow(val,(double) ((VFloat) v).val);
            return this;
        }
        throw new IncompatibleType();
    }

    @Override
    public boolean eq(Value v) throws IncompatibleType {
        if(v instanceof VDouble){
            return val == ((VDouble) v).val;
        }
        if(v instanceof VInt){
            return val == (double) ((VInt) v).val;
        }
        if(v instanceof VFloat){
            return val == (double) ((VFloat) v).val;
        }
        throw new IncompatibleType();
    }

    @Override
    public boolean gte(Value v) throws IncompatibleType {
        if(v instanceof VDouble){
            return val >= ((VDouble) v).val;
        }
        if(v instanceof VInt){
            return val >= (double) ((VInt) v).val;
        }
        if(v instanceof VFloat){
            return val >= (double) ((VFloat) v).val;
        }
        throw new IncompatibleType();
    }

    @Override
    public boolean lte(Value v) throws IncompatibleType {
        if(v instanceof VDouble){
            return val <= ((VDouble) v).val;
        }
        if(v instanceof VInt){
            return val <= (double) ((VInt) v).val;
        }
        if(v instanceof VFloat){
            return val <= (double) ((VFloat) v).val;
        }
        throw new IncompatibleType();
    }

    @Override
    public boolean gt(Value v) throws IncompatibleType {
        if(v instanceof VDouble){
            return val > ((VDouble) v).val;
        }
        if(v instanceof VInt){
            return val > (double) ((VInt) v).val;
        }
        if(v instanceof VFloat){
            return val > (double) ((VFloat) v).val;
        }
        throw new IncompatibleType();
    }

    @Override
    public boolean lt(Value v) throws IncompatibleType {
        if(v instanceof VDouble){
            return val < ((VDouble) v).val;
        }
        if(v instanceof VInt){
            return val < (double) ((VInt) v).val;
        }
        if(v instanceof VFloat){
            return val < (double) ((VFloat) v).val;
        }
        throw new IncompatibleType();
    }

    @Override
    public boolean neq(Value v) throws IncompatibleType {
        if(v instanceof VDouble){
            return val != ((VDouble) v).val;
        }
        if(v instanceof VInt){
            return val != (double) ((VInt) v).val;
        }
        if(v instanceof VFloat){
            return val != (double) ((VFloat) v).val;
        }
        throw new IncompatibleType();
    }

    @Override
    public boolean equals(Object other) {
        return this.hashCode() == other.hashCode();
    }

    @Override
    public int hashCode() {
        int r = Double.valueOf(val).hashCode() * 10 + 2;
        return r;
    }

    @Override
    public VDouble create(String s) {
        VDouble r = new VDouble(Double.parseDouble(s));
        return r;
    }

    @Override
    public Object clone() throws CloneNotSupportedException{
        Object cln = new VDouble(val);
        return cln;
    }

    @Override
    public Number toNumber() {
        return new Number() {
            @Override
            public int intValue() {
                return (int)val;
            }

            @Override
            public long longValue() {
                return (long)val;
            }

            @Override
            public float floatValue() {
                return (float)val;
            }

            @Override
            public double doubleValue() {
                return val;
            }
        };
    }

    //do tego jebanego konstruktora coovalue w SparseColumn (nie ma dostępu do pola val przy obiekcie Value)\
    @Override
    public Object getVal(){
        return this.val;
    }
}
