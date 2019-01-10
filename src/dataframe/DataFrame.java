package dataframe;

import java.io.*;
import java.util.*;

public class DataFrame implements Cloneable{
    public String[] cnames;
    public List<Class<? extends Value>> ctypes;
    public Column[] cols;
    public int width;
    public int height;

    //lab 1 / 2
    public DataFrame(String[] c_names,List<Class<? extends Value>> c_types){
        cnames = c_names;
        ctypes = c_types;
        width = c_names.length;
        cols = new Column[width];
        for(int i=0;i<width;i++){
            cols[i] = new Column(c_names[i], c_types.get(i));
        }
        height = 0;
    }

    public DataFrame(String filename, List<Class<? extends Value>> c_types,boolean header) throws IOException, IllegalAccessException, InstantiationException, InvalidWidth {
        ctypes = c_types;
        width = c_types.size();
        File file = new File(filename);
        BufferedReader br = new BufferedReader(new FileReader(file));
        String firstLine = br.readLine();
        int realWidth = 0;
        for(int i=0;i<firstLine.length();i++){
            if(firstLine.charAt(i) == ',')realWidth++;
        }
        if(realWidth+1 != width){
            throw new InvalidWidth();
        }

        br = new BufferedReader(new FileReader(file));
        cnames = new String[width];
        if(!header){
            Scanner reader = new Scanner(System.in);
            System.out.println("Podaj nazwy kolumn: ");
            for(int i =0;i<width;i++){
                cnames[i] = reader.next();
            }
        }else{
            String hdr = br.readLine();
            int it = 0;
            int pos = 0;
            for(int i=0;i<hdr.length();i++){
                if(hdr.charAt(i)==','){
                    cnames[it] = hdr.substring(pos,i);
                    pos = i+1;
                    it++;
                }
                if(i==hdr.length()-1){
                    cnames[it] = hdr.substring(pos);
                }
            }
        }
        cols = new Column[width];
        for(int i=0;i<width;i++){
            cols[i] = new Column(cnames[i], ctypes.get(i));
        }
        height =0;
        String line;
        while((line = br.readLine()) != null){
            int it =0;
            int pos =0;
            for(int i=0;i<line.length();i++){
                if(line.charAt(i)==','){
                    this.addS(line.substring(pos,i),it);
                    pos = i+1;
                    it++;
                }
                if(i==line.length()-1){
                    this.addS(line.substring(pos),it);
                }
            }
        }
    }

    public void addS(String s,int col_id) throws InstantiationException, IllegalAccessException {
        cols[col_id].addS(s);
        height = cols[col_id].h;
    }

    public void  Add(Value val,int col_id){
        cols[col_id].Add(val);
        height = cols[col_id].h;
    }

    public int size(){
        return height;
    }
    public Column get(String colname){
        for(Column n: cols){
            if(n.name == colname){
                return n;
            }
        }
        return null;
    }

    public DataFrame iloc(int i){
        DataFrame dfr = new DataFrame(cnames,ctypes);
        for(int it=0;it<width;it++){
            dfr.Add(cols[it].col.get(i),it);
        }
        return dfr;
    }

    public DataFrame iloc(int from, int to){
        DataFrame dfr = new DataFrame(cnames,ctypes);
        for(int i=from;i<=to;i++){
            for(int it=0;it<width;it++){
                dfr.Add(cols[it].col.get(i),it);
            }
        }
        return dfr;
    }

    public DataFrame get(String[] cols,boolean copy) throws CloneNotSupportedException{
        List<Class<? extends Value>> colst = new ArrayList<>();
        int it =0;
        for(Column n: this.cols){
            if(n.name == cols[it]){
                colst.add(n.type);
                it++;
            }
        }
        DataFrame dfr = new DataFrame(cols,colst);
        if(copy){
            for(int i=0;i<dfr.width;i++){
                for(Column n: this.cols){
                    if(n.name == dfr.cnames[i]) dfr.cols[i] = (Column)n.clone();
                }
            }
        }else{
            for(int i=0;i<dfr.width;i++){
                for(Column n: this.cols){
                    dfr.cols[i] = n;
                }
            }
        }
        return dfr;
    }

    public void print(){
        for(String n: cnames) System.out.print(n + " ");
        System.out.println();
        for(int i = 0; i< height; i++){
            for(int j=0;j<width;j++){
                System.out.print(cols[j].col.get(i).toString() + " ");
            }
            System.out.println();
        }
        System.out.println();
    }

    //lab 4
    public class GroupedDF implements Groupby{
        LinkedList<DataFrame> dataframes;
        ArrayList<Integer> key_id;

        GroupedDF(){
            key_id = new ArrayList<>();
            dataframes = new LinkedList<>();
        }

        @Override
        public DataFrame max() {
            DataFrame result = new DataFrame(dataframes.peekFirst().cnames,dataframes.peekFirst().ctypes);
            for(DataFrame n: dataframes){
                for(int i=0;i<n.width;i++){
                    if(key_id.contains(i)){
                        result.Add(n.cols[i].col.get(0),i);
                    }else{
                        Value maxv = new VString();
                        for(int k = 0; k<n.cols[i].col.size(); k++){
                            if(k==0)maxv = n.cols[i].col.get(0);
                            else{
                                try {
                                    if(n.cols[i].col.get(k).gt(maxv)) maxv = n.cols[i].col.get(k);
                                } catch (IncompatibleType incompatibleType) {
                                    incompatibleType.printStackTrace();
                                }
                            }
                        }
                        result.Add(maxv,i);
                    }
                }
            }
            return result;
        }

        @Override
        public DataFrame min() {
            DataFrame result = new DataFrame(dataframes.peekFirst().cnames,dataframes.peekFirst().ctypes);
            for(DataFrame n: dataframes){
                for(int j=0;j<n.width;j++){
                    if(key_id.contains(j)){
                        result.Add(n.cols[j].col.get(0),j);
                    }else{
                        Value minv = new VString();
                        for(int k = 0; k<n.cols[j].col.size(); k++){
                            if(k==0)minv = n.cols[j].col.get(0);
                            else{
                                try {
                                    if(n.cols[j].col.get(k).lt(minv)) minv = n.cols[j].col.get(k);
                                } catch (IncompatibleType incompatibleType) {
                                    incompatibleType.printStackTrace();
                                }
                            }
                        }
                        result.Add(minv,j);
                    }
                }
            }
            return result;
        }

        @Override
        public DataFrame mean() {
            DataFrame result = new DataFrame(dataframes.peekFirst().cnames,dataframes.peekFirst().ctypes);
            for(DataFrame n: dataframes){
                for(int j=0;j<n.width;j++){
                    if(key_id.contains(j)){
                        result.Add(n.cols[j].col.get(0),j);
                    }else{
                        if(n.cols[j].col.get(0) instanceof VString || n.cols[j].col.get(0) instanceof VDateTime){
                            result.Add(new VString("NA"),j);
                        }else{
                            Value mean = new VDouble(0);
                            int k;
                            for(k=0; k<n.cols[j].col.size(); k++){
                                try {
                                    mean.add(n.cols[j].col.get(k));
                                } catch (IncompatibleType incompatibleType) {
                                    incompatibleType.printStackTrace();
                                }
                            }
                            try {
                                mean.div(new VDouble(k));
                            } catch (IncompatibleType incompatibleType) {
                                incompatibleType.printStackTrace();
                            } catch (DivisionByZero divisionByZero) {
                                divisionByZero.printStackTrace();
                            }
                            result.Add(mean,j);
                        }
                    }
                }
            }
            return result;
        }

        @Override
        public DataFrame std() {
            DataFrame result = new DataFrame(dataframes.peekFirst().cnames,dataframes.peekFirst().ctypes);
            for(DataFrame n: dataframes){
                for(int j=0;j<n.width;j++){
                    if(key_id.contains(j)){
                        result.Add(n.cols[j].col.get(0),j);
                    }else{
                        if(n.cols[j].col.get(0) instanceof VString || n.cols[j].col.get(0) instanceof VDateTime){
                            result.Add(new VString("NA"),j);
                        }else{
                            VDouble mean = new VDouble(0);
                            int k;
                            for(k=0; k<n.cols[j].col.size(); k++){
                                try {
                                    mean.add(n.cols[j].col.get(k));
                                } catch (IncompatibleType incompatibleType) {
                                    incompatibleType.printStackTrace();
                                }
                            }
                            try {
                                mean.div(new VDouble(k));
                            } catch (DivisionByZero divisionByZero) {
                                divisionByZero.printStackTrace();
                            }
                            VDouble std = new VDouble(0);
                            for(int l = 0; l<n.cols[j].col.size(); l++){
                                VDouble tmp = new VDouble(mean.val);
                                try {
                                    tmp.sub(n.cols[j].col.get(l));
                                } catch (IncompatibleType incompatibleType) {
                                    incompatibleType.printStackTrace();
                                }
                                try {
                                    std.add(tmp.mul(tmp));
                                } catch (IncompatibleType incompatibleType) {
                                    incompatibleType.printStackTrace();
                                }
                            }
                            try {
                                std.div(new VDouble(k-1));
                            } catch (DivisionByZero divisionByZero) {
                                divisionByZero.printStackTrace();
                            }
                            std.val = Math.sqrt(std.val);
                            result.Add(std,j);
                        }
                    }
                }
            }
            return result;
        }

        @Override
        public DataFrame sum() {
            DataFrame result = new DataFrame(dataframes.peekFirst().cnames,dataframes.peekFirst().ctypes);
            for(DataFrame n: dataframes){
                for(int j=0;j<n.width;j++){
                    if(key_id.contains(j)){
                        result.Add(n.cols[j].col.get(0),j);
                    }else{
                        if(n.cols[j].col.get(0) instanceof VString || n.cols[j].col.get(0) instanceof VDateTime){
                            result.Add(new VString("NA"),j);
                        }else{
                            Value sum = new VDouble(0);
                            int k;
                            for(k=0; k<n.cols[j].col.size(); k++){
                                try {
                                    sum.add(n.cols[j].col.get(k));
                                } catch (IncompatibleType incompatibleType) {
                                    incompatibleType.printStackTrace();
                                }
                            }
                            result.Add(sum,j);
                        }
                    }
                }
            }
            return result;
        }

        @Override
        public DataFrame var() {
            DataFrame result = new DataFrame(dataframes.peekFirst().cnames,dataframes.peekFirst().ctypes);
            for(DataFrame n: dataframes){
                for(int j=0;j<n.width;j++){
                    if(key_id.contains(j)){
                        result.Add(n.cols[j].col.get(0),j);
                    }else{
                        if(n.cols[j].col.get(0) instanceof VString || n.cols[j].col.get(0) instanceof VDateTime){
                            result.Add(new VString("NA"),j);
                        }else{
                            VDouble mean = new VDouble(0);
                            int k;
                            for(k=0; k<n.cols[j].col.size(); k++){
                                try {
                                    mean.add(n.cols[j].col.get(k));
                                } catch (IncompatibleType incompatibleType) {
                                    incompatibleType.printStackTrace();
                                }
                            }
                            try {
                                mean.div(new VDouble(k));
                            } catch (DivisionByZero divisionByZero) {
                                divisionByZero.printStackTrace();
                            }
                            VDouble var = new VDouble(0);
                            for(int l = 0; l<n.cols[j].col.size(); l++){
                                VDouble tmp = new VDouble(mean.val);
                                try {
                                    tmp.sub(n.cols[j].col.get(l));
                                } catch (IncompatibleType incompatibleType) {
                                    incompatibleType.printStackTrace();
                                }
                                try {
                                    var.add(tmp.mul(tmp));
                                } catch (IncompatibleType incompatibleType) {
                                    incompatibleType.printStackTrace();
                                }
                            }
                            try {
                                var.div(new VDouble(k-1));
                            } catch (DivisionByZero divisionByZero) {
                                divisionByZero.printStackTrace();
                            }
                            result.Add(var,j);
                        }
                    }
                }
            }
            return result;
        }

        @Override
        public DataFrame apply(Applyable a) {
            return null;
        }

        public void linkGroupedLists(GroupedDF other){
            this.dataframes.addAll(other.dataframes);
            for(int n: other.key_id){
                if(!key_id.contains(n)){
                    key_id.add(n);
                }
            }
        }
    }

    public GroupedDF groupbyS(String colname){

        GroupedDF r = new GroupedDF();
        int it = 0;

        while(!cols[it].name.equals(colname)) it++;
        r.key_id.add(it);
        for(int i = 0; i< height; i++){
            if(i==0){
                DataFrame ndf = this.iloc(i);
                r.dataframes.add(ndf);
            }else{
                boolean DFGroupFound = false;
                int l = 0;
                for(DataFrame n: r.dataframes){
                    try {
                        if(this.cols[it].col.get(i).eq(n.cols[it].col.get(0))){
                            DFGroupFound = true;
                            break;
                        }
                    } catch (IncompatibleType incompatibleType) {
                        incompatibleType.printStackTrace();
                    }
                    l++;
                }
                if(DFGroupFound){
                    for(int j=0;j<width;j++){
                        r.dataframes.get(l).Add(this.cols[j].col.get(i),j);
                    }
                }else{
                    DataFrame ndf = this.iloc(i);
                    r.dataframes.add(ndf);
                }
            }
        }
        return r;
    }

    public GroupedDF groupby(String colnames[]){
        GroupedDF result = new GroupedDF();
        for(int i=0;i<colnames.length;i++){
            if(i == 0){
                result = this.groupbyS(colnames[0]);
            }else{
                GroupedDF prev_res = result;
                result = new GroupedDF();
                result.key_id = prev_res.key_id;
                for(DataFrame n: prev_res.dataframes){
                    GroupedDF tmp = n.groupbyS(colnames[i]);
                    result.linkGroupedLists(tmp);
                }
            }
        }
        return result;
    }

    public GroupedDF groupby(){
        GroupedDF r = new GroupedDF();
        r.dataframes.add(this);
        return r;
    }

    //main
    public static void main(String[] args)  {
        ArrayList<Class<? extends Value>> types = new ArrayList<>();
        /*
        ct.add(dataframe.VInt.class);
        ct.add(dataframe.VInt.class);
        dataframe.SparseDataFrame dfs1 = new dataframe.SparseDataFrame(new String[] {"kol1","kol2"},ct , new dataframe.VInt(0));
        dfs1.sFilld();
        dataframe.DataFrame df1 = dfs1.toDense();
        df1.print();
        String a[] = {"kol1"};
        df1.groupby(a).max().print();*/
        //GroupedDF gdf = df1.groupby(a);
        //gdf.dataframes.get(0).print();
        //gdf.dataframes.get(1).print();*/

        types.add(VString.class);
        types.add(VDateTime.class);
        types.add(VDouble.class);
        types.add(VDouble.class);
        try {
            DataFrame df1 = new DataFrame("groupby.csv",types,true);
            DataFrame df2 = df1.iloc(100000);
            df2.groupby(new String[]{"date"}).mean().print();
        } catch (IllegalAccessException | InstantiationException | IOException | InvalidWidth e) {
            e.printStackTrace();
        }
    }

    public static int powr(int a,int b){
        if(b == 0){
            return 1;
        }
        if(b == 1){
            return a;
        }
        if(b%2 == 0){
            return powr(a,b/2)*powr(a,b/2);
        }else{
            return powr(a,b-1) * a;
        }
    }
}
