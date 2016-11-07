
import com.mongodb.BasicDBObject;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestMainClass {
    // JDBC driver name and database URL
    private static final String JDBC_DRIVER = "oracle.jdbc.driver.OracleDriver";

    public static Connection conn = null;
    public static Statement stmt = null;

    public static final int CONNECTION_SUCCESS = 0;
    public static final int CONNECTION_ERROR_SQL = 1;
    public static final int CONNECTION_ERROR_FORNAME = 2;

	public static void main(String[] args) throws Exception {

        /*
        if(args.length != 4){
            System.out.println("USAGE: TestMainClass database_url:port login pass table_name");
            return;
        }

        String db_url = "jdbc:oracle:thin:@" + args[0] + ":xe";
        String user = args[1];
        String pass = args[2];
        String targetTable = args[3];
        /*/

        String db_url = "jdbc:oracle:thin:@localhost:1521:xe";
        String user = "g8937204";
        String pass = "4703es";
        String targetTable = "LE02CIDADE";

        /**/

        //connects to SQL database
        startConnection(db_url, user, pass);

        doThingy("LE01ESTADO");
        doThingy("LE02CIDADE");
        doThingy("LE03ZONA");
        doThingy("LE04BAIRRO");
        doThingy("LE05URNA");
        doThingy("LE06SESSAO");
        doThingy("LE07PARTIDO");
        doThingy("LE08CANDIDATO");
        doThingy("LE09CARGO");
        doThingy("LE10CANDIDATURA");
        doThingy("LE11PLEITO");
        doThingy("LE12PESQUISA");
        doThingy("LE13INTENCAODEVOTO");


        closeConnection();

	}

	public static void doThingy(String targetTable) throws SQLException {
        //fetches table info
        ResultSet rs = stmt.executeQuery("select * from " + targetTable);

        List<String> primaryKeys = getPrimaryKeys(targetTable);
        ArrayList<ForeignKeyBundle> foreignKeyBundles = getForeignKeys(targetTable);
        ArrayList<String> columnNames = getColumnNames(rs);

        ArrayList<String> allForeignKeys = new ArrayList<>();
        foreignKeyBundles.forEach(fkb -> fkb.fks.forEach(fk -> allForeignKeys.add(fk.fkVariable)));

        System.out.println("db.createCollection(\""+ targetTable +"\")");
        while(rs.next()){
            BasicDBObject obj = new BasicDBObject();

            //creating ID
            String id = makeId(rs, primaryKeys);
            obj.put("_id", id);

            //creating Foreign Keys
            for(ForeignKeyBundle foreignKeyBundle : foreignKeyBundles){
                String foreignKeyString = "";
                for(ForeignKey foreignKey : foreignKeyBundle.fks){
                    foreignKeyString += "_" + rs.getString(foreignKey.fkVariable);
                }
                obj.put(foreignKeyBundle.fks.get(0).fkName, foreignKeyString);
            }

            //creating rest of variables
            for(String columnName : columnNames){
                if(!allForeignKeys.contains(columnName)){
                    obj.put(columnName, rs.getString(columnName));
                }
            }

            String bsonString = obj.toString();
            System.out.println(bsonString);
        }
    }

	public static ArrayList<String> getColumnNames(ResultSet rs) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();
        ArrayList<String> result = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++ ) {
            result.add(rsmd.getColumnName(i));
        }
        return result;
    }

	public static class ForeignKeyBundle{
        ArrayList <ForeignKey> fks = new ArrayList<>();
    }
    public static class ForeignKey{
        public String fkName, referencedTable, fkVariable, pkVariable;
    }
    private static ArrayList<ForeignKeyBundle> getForeignKeys(String tableName) throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet rs = metaData.getImportedKeys(conn.getCatalog(), null, tableName);

        ArrayList<ForeignKey> foreignKeys = new ArrayList<>();
        while (rs.next()) {
            ForeignKey fk = new ForeignKey();
            fk.fkName = rs.getString("FK_NAME");
            fk.referencedTable = rs.getString("PKTABLE_NAME");
            fk.fkVariable = rs.getString("FKCOLUMN_NAME");
            fk.pkVariable = rs.getString("PKCOLUMN_NAME");
            foreignKeys.add(fk);
        }

        ArrayList<ForeignKeyBundle> foreignKeyBundles = new ArrayList<>();
        foreignKeys.stream().map((ForeignKey fk) -> fk.fkName).forEach(fkName -> {
            ForeignKeyBundle fkb = new ForeignKeyBundle();
            foreignKeys.
                    stream().
                    filter(fkFiltered -> fkFiltered.fkName.equals(fkName)).
                    forEach(fkb.fks::add);
            //fkb.fks
            Collections.sort(fkb.fks, (ForeignKey fk, ForeignKey fk2) -> fk.pkVariable.compareTo(fk2.pkVariable));

            foreignKeyBundles.add(fkb);
        });

        return foreignKeyBundles;
    }

	public static List<String> getPrimaryKeys(String tableName) throws SQLException {
        ResultSet rs = conn.getMetaData().getPrimaryKeys(null, null, tableName);

        ArrayList<String> result = new ArrayList<>();
        while(rs.next()){
            String pkColumnName = rs.getString("COLUMN_NAME");
            result.add(pkColumnName);
        }
        Collections.sort(result);

        return result;
    }

	public static String makeId(ResultSet rs, List<String> primaryKeys) throws SQLException {
        String result = "";

        for(int i = 0; i < primaryKeys.size(); i++){
            result += "_" + rs.getString(primaryKeys.get(i));
        }

        return result;
    }

    public static int startConnection(String db_url, String user, String pass){
        try{
            //STEP 2: Register JDBC driver
            Class.forName(JDBC_DRIVER);

            //STEP 3: Open a connection
            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(db_url,user,pass);

            //STEP 4: Execute a query
            System.out.println("Creating statement...");
            stmt = conn.createStatement();

            return CONNECTION_SUCCESS;
        }catch(SQLException se){
            //Handle errors for JDBC
            se.printStackTrace();
            return CONNECTION_ERROR_SQL;
        }catch(Exception e){
            //Handle errors for Class.forName
            e.printStackTrace();
            return CONNECTION_ERROR_FORNAME;
        }
    }

    public static void closeConnection(){
        System.out.println("Closing connection...");
        try{
            if(stmt!=null){
                stmt.close();
            }
        }catch(SQLException se2){
            se2.printStackTrace();
        }

        try{
            if(conn!=null){
                conn.close();
            }
        }catch(SQLException se){
            se.printStackTrace();
        }

        stmt = null;
        conn = null;
    }
}
