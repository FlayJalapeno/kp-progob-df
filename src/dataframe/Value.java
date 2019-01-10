package dataframe;

public abstract class Value {

    public abstract Object getVal();
    public abstract String toString();
    public abstract Value add(Value v) throws IncompatibleType;
    public abstract Value sub(Value v) throws IncompatibleType;
    public abstract Value mul(Value v) throws IncompatibleType;
    public abstract Value div(Value v) throws IncompatibleType, DivisionByZero;
    public abstract Value pow(Value v) throws IncompatibleType;
    public abstract boolean eq(Value v) throws IncompatibleType;
    public abstract boolean gte(Value v) throws IncompatibleType;
    public abstract boolean lte(Value v) throws IncompatibleType;
    public abstract boolean gt(Value v) throws IncompatibleType;
    public abstract boolean lt(Value v) throws IncompatibleType;
    public abstract boolean neq(Value v) throws IncompatibleType;
    public abstract boolean equals(Object other);
    public abstract int hashCode();
    public abstract Number toNumber();
    public abstract <T> T create(String s);
}
