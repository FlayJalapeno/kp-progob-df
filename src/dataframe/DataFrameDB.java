package dataframe;

import org.sqlite.SQLiteException;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.sql.*;

public class DataFrameDB extends DataFrame {
    
    public static int lastFileKey = 0;
    public static String dbURL = "jdbc:sqlite:/home/kornel/studia/kp-progob-df/dfdb" + lastFileKey + ".db";

    private Connection con;
    private Statement stat;

    public DataFrameDB(DataFrame df, int pkey, boolean newFile){
        super(df.cnames,df.ctypes);
        int it = 0;
        boolean freeFKey = false;
        if(!newFile) freeFKey = true;
        while(!freeFKey){
            String fileChecker = "/home/kornel/studia/kp-progob-df/dfdb" + it + ".db";
            File checkFile = new File(fileChecker);
            if(checkFile.exists()){
                it++;
            }else{
                DataFrameDB.lastFileKey = it;
                freeFKey = true;
            }
        }
        DataFrameDB.dbURL  = "jdbc:sqlite:/home/kornel/studia/kp-progob-df/dfdb" + lastFileKey + ".db";
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException e) {
            System.err.println("Brak sterownika JDBC");
            e.printStackTrace();
        }

        try {
            con = DriverManager.getConnection(dbURL);
            stat = con.createStatement();
            System.out.println("Połączono z bazą danych!");
        } catch (SQLException e) {
            System.err.println("Problem z otwarciem polaczenia");
            e.printStackTrace();
        }

        String create = "CREATE TABLE IF NOT EXISTS dataframe (\n";
        for(int i=0;i<df.width;i++){
            create += df.cnames[i] + " ";
            if(df.ctypes.get(i) == VInt.class){
                create += "INT";
            }
            else if(df.ctypes.get(i) == VDouble.class || df.ctypes.get(i) == VFloat.class){
                create += "NUMERIC(24,12)";
            }
            else if(df.ctypes.get(i) == VDateTime.class){
                create += "DATE";
            }
            else if(df.ctypes.get(i) !=null){
                create += "VARCHAR(255)";
            }
            if(pkey >= 0 && i==pkey){
                create += " NOT NULL PRIMARY KEY,\n";
            }
            else{
                create += ",\n";
            }
            if(i==df.width-1){
                create = create.substring(0,create.length()-2);
                //create += "PRIMARY KEY(" + df.cnames[0] + ")\n";
                create += ");";
                //System.out.println(create);
            }
        }
        try{
            stat.executeUpdate(create);
            for(int i = 0; i<df.height; i++){
                String insert = "INSERT INTO dataframe VALUES (";
                for(int j=0;j<df.width;j++){
                    if(df.ctypes.get(j) == VDouble.class || df.ctypes.get(j) == VFloat.class || df.ctypes.get(j) == VInt.class){
                        insert = insert + df.cols[j].col.get(i).toString() + ", ";
                    }else if(df.ctypes.get(j) != null){
                        insert = insert + "'" + df.cols[j].col.get(i).toString() + "', ";
                    }
                    if(j==df.width-1){
                        insert = insert.substring(0,insert.length()-2);
                        insert += ");";
                        //System.out.println(insert);
                    }
                }
                stat.executeUpdate(insert);
            }
        } catch (SQLiteException e) {
            System.err.println("Nie powiodło się wstawianie do tabeli! Sprawdź, czy wartości klucza się nie powtarzają!");
        } catch (SQLException e){
            e.printStackTrace();
        }
    }

    public void max(int[] sel_cols, int groupBy){
        String maxRequest = "SELECT ";
        if(groupBy > -1){
            maxRequest += cnames[groupBy] + ", ";
        }
        for(int i:sel_cols){
            maxRequest += "MAX(" + cnames[i] + ")";
            if(i != sel_cols[sel_cols.length - 1]){
                maxRequest += ", ";
            }
        }
        maxRequest += " FROM dataframe";
        if(groupBy > -1){
            maxRequest += " GROUP BY " + cnames[groupBy] + ";";
        } else {
            maxRequest += ";";
        }
        try {
            ResultSet rs = stat.executeQuery(maxRequest);
            String print = "";
            if(groupBy > -1){
                print += cnames[groupBy] + "\t";
            }
            for(int i:sel_cols){
                print += "max(" + cnames[i] + ")\t";
            }
            print += "\n";
            while(rs.next()){
                if(groupBy < 0) {
                    for (int i = 0; i < sel_cols.length; i++) {
                        print += rs.getString(i + 1) + "\t\t";
                    }
                } else {
                    for (int i = 0; i < sel_cols.length + 1; i++) {
                        print += rs.getString(i + 1) + "\t\t";
                    }
                }
                print += "\n";
            }
            System.out.println(print);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public void min(int[] sel_cols, int groupBy){
        String minRequest = "SELECT ";
        if(groupBy > -1){
            minRequest += cnames[groupBy] + ", ";
        }
        for(int i:sel_cols){
            minRequest += "MIN(" + cnames[i] + ")";
            if(i != sel_cols[sel_cols.length - 1]){
                minRequest += ", ";
            }
        }
        minRequest += " FROM dataframe";
        if(groupBy > -1){
            minRequest += " GROUP BY " + cnames[groupBy] + ";";
        } else {
            minRequest += ";";
        }
        try {
            ResultSet rs = stat.executeQuery(minRequest);
            String print = "";
            if(groupBy > -1){
                print += cnames[groupBy] + "\t";
            }
            for(int i:sel_cols){
                print += "min(" + cnames[i] + ")\t";
            }
            print += "\n";
            while(rs.next()){
                if(groupBy < 0) {
                    for (int i = 0; i < sel_cols.length; i++) {
                        print += rs.getString(i + 1) + "\t\t";
                    }
                } else {
                    for (int i = 0; i < sel_cols.length + 1; i++) {
                        print += rs.getString(i + 1) + "\t\t";
                    }
                }
                print += "\n";
            }
            System.out.println(print);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
    public void groupby1(){

    }

    @Override
    public void print() {
        try {
            ResultSet rs = stat.executeQuery("SELECT * FROM dataframe");
            String print = "";
            while(rs.next()){
                for(int i=0;i<width;i++){
                    print = print + rs.getString(i+1) + "\t";
                }
                print = print + "\n";
            }
            System.out.println(print);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args){

        ArrayList<Class<? extends Value>> ct = new ArrayList<>();
        ct.add(VInt.class);
        ct.add(VString.class);
        ct.add(VDouble.class);
        try {
            DataFrame dt1 = new DataFrame("./data1.csv",ct,true);
            DataFrameDB dtd1 = new DataFrameDB(dt1, 0, false);
            dtd1.print();
            dtd1.max(new int[]{0,2}, 1);
            dtd1.min(new int[]{0,2}, 1);
        } catch (IOException | InvalidWidth | InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }
        String fileNameString = "Plik z (ostatnią stworzoną) bazą danych: dfdb"+DataFrameDB.lastFileKey+".db";
        System.out.println(fileNameString);

        /*
        ArrayList<Class<? extends Value>> types = new ArrayList<>();

        types.add(VString.class);
        types.add(VDateTime.class);
        types.add(VDouble.class);
        types.add(VDouble.class);
        try {
            DataFrame df1 = new DataFrame("groupby.csv",types,true);
            DataFrameDB dtd1 = new DataFrameDB(df1, -1, true);
            dtd1.print();
        } catch (IllegalAccessException | InstantiationException | IOException | InvalidWidth e) {
            e.printStackTrace();
        }
        */
    }
}
