import com.mongodb.BasicDBObject;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

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

        //Creates basic objects
        ArrayList<BasicDBObject> lEstado = do1ToN("LE01ESTADO");
        ArrayList<BasicDBObject> lCidade = do1ToN("LE02CIDADE");
        //ArrayList<BasicDBObject> lZona = do1ToN("LE03ZONA");
        //ArrayList<BasicDBObject> lBairro = do1ToN("LE04BAIRRO");
        //ArrayList<BasicDBObject> lUrna = do1ToN("LE05URNA");
        //ArrayList<BasicDBObject> lSessao = do1ToN("LE06SESSAO");
        //ArrayList<BasicDBObject> lPartido = do1ToN("LE07PARTIDO");
        //ArrayList<BasicDBObject> lCandidato = do1ToN("LE08CANDIDATO");
        //ArrayList<BasicDBObject> lCargo = do1ToN("LE09CARGO");
        //ArrayList<BasicDBObject> lCandidatura = do1ToN("LE10CANDIDATURA", new String[]{});
        //ArrayList<BasicDBObject> lPesquisa = do1ToN("LE12PESQUISA");

        //ArrayList<BasicDBObject> lPleito = do1ToN("LE11PLEITO");
        //ArrayList<BasicDBObject> lIntencaoDeVoto = do1ToN("LE13INTENCAODEVOTO");

        //Adds NToNRelations
        //createNToNRelations(lPleito, lSessao, lCandidatura, "FKSESSAOCANDIDATURA", "FKCANDIDATURASESSAO", "Candidaturas", "Sessoes");
        //createNToNRelations(lIntencaoDeVoto, lPesquisa, lCandidatura, "FKPESQUISACANDIDATURA", "FKCANDIDATURAPESQUISA", "Candidaturas", "Pesquisas");

        //Does embedding
        //lCidade = embed(lCidade, lBairro, "Bairros", "FKCIDADEBAIRRO");
        lEstado = embed(lCidade, lEstado, "Cidades", "FKESTADOCIDADE");
        //lZona   = embed(lZona,   lSessao, "Sessoes", "FKZONASESSAO");

        //these are now useless
        //lCidade = null;
        //lZona = null;
        //lBairro = null

        lEstado.forEach(System.out::println);
        //lCandidatura.forEach(System.out::println);

        //Closes connection
        closeConnection();
	}

    public static class TwoStrings{
        String name, content;

        public TwoStrings(String name, String content){
            this.name = name;
            this.content = content;
        }
    }

	public static class ReceiverSideOfNToNRelation{
        public String id;
        public ArrayList<ArrayList<TwoStrings>> references = new ArrayList<>();

        public ReceiverSideOfNToNRelation(String id){
            this.id = id;
        }

        public ArrayList<BasicDBObject> asBasicDBObjectArray(){
            ArrayList<BasicDBObject> basicDBObjectArray = new ArrayList<>();
            references.forEach((obj)-> basicDBObjectArray.add(toBasicDBObject(obj)));

            return basicDBObjectArray;
        }

        private BasicDBObject toBasicDBObject(ArrayList<TwoStrings> array){
            BasicDBObject basicDBObject = new BasicDBObject();
            array.forEach((obj)-> basicDBObject.put(obj.name, obj.content));

            return basicDBObject;
        }
    }

    public static class ExtraSideOfNToNRelation{
        public String id;
        public ArrayList<String> references = new ArrayList<>();

        public ExtraSideOfNToNRelation(String id){
            this.id = id;
        }
    }

    public static void createNToNRelations(ArrayList<BasicDBObject> tableWithReferences,
                                           ArrayList<BasicDBObject> tableToReceiveExtraFields,
                                           ArrayList<BasicDBObject> tableExtra,
                                           String fkNameTowardsReceiver,
                                           String fkNameTowardsExtra,
                                           String newFieldInReceiver,
                                           String newFieldInExtra){
        ArrayList<ReceiverSideOfNToNRelation> receiverSideOfNToNRelations = new ArrayList<>();
        ArrayList<ExtraSideOfNToNRelation> extraSideOfNToNRelations = new ArrayList<>();

        tableWithReferences.forEach((objWithReferences)->{
            //getting the receiver
            String receiverId = (String) objWithReferences.get(fkNameTowardsReceiver);
            ReceiverSideOfNToNRelation receiver;
            Optional<ReceiverSideOfNToNRelation> optionalReceiver =
                    receiverSideOfNToNRelations
                            .stream()
                            .filter((receiverSideOfNToNRelation -> receiverSideOfNToNRelation.id.equals(receiverId)))
                            .findFirst();
            if(optionalReceiver.isPresent()){
                receiver = optionalReceiver.get();
            }else{
                receiver = new ReceiverSideOfNToNRelation(receiverId);
                receiverSideOfNToNRelations.add(receiver);
            }

            //getting the extra
            String extraId = (String) objWithReferences.get(fkNameTowardsExtra);
            ExtraSideOfNToNRelation extra;
            Optional<ExtraSideOfNToNRelation> optionalExtra =
                    extraSideOfNToNRelations
                            .stream()
                            .filter((extraSideOfNToNRelation -> extraSideOfNToNRelation.id.equals(extraId)))
                            .findFirst();
            if(optionalExtra.isPresent()){
                extra = optionalExtra.get();
            }else{
                extra = new ExtraSideOfNToNRelation(extraId);
                extraSideOfNToNRelations.add(extra);
            }

            ArrayList<TwoStrings> twoStringsArrayList = new ArrayList<>();
            objWithReferences.forEach((String name, Object value) -> {
                if(!name.equals("_id") && !name.equals(fkNameTowardsReceiver) && !name.equals(fkNameTowardsExtra)) {
                    //receiver.references.add(new TwoStrings(name, (String) value));
                    twoStringsArrayList.add(new TwoStrings(name, (String) value));
                }else if(name.equals(fkNameTowardsExtra)){
                    //receiver.references.add(new TwoStrings("_id", (String) value));
                    twoStringsArrayList.add(new TwoStrings("_id", (String) value));
                }else if(name.equals(fkNameTowardsReceiver)){
                    extra.references.add((String) value);
                }
            });
            receiver.references.add(twoStringsArrayList);
        });

        receiverSideOfNToNRelations.forEach((receiverSideOfNToNRelation -> {
            BasicDBObject obj = tableToReceiveExtraFields
                    .stream()
                    .filter(basicDBObject -> {
                        //System.out.println(basicDBObject.get("_id") + " == " + receiverSideOfNToNRelation.id);
                        return basicDBObject.get("_id").equals(receiverSideOfNToNRelation.id);
                    })
                    .findAny()
                    .get();
            obj.put(newFieldInReceiver, receiverSideOfNToNRelation.asBasicDBObjectArray().toArray());
        }));

        extraSideOfNToNRelations.forEach((extraSideOfNToNRelation -> {
            BasicDBObject obj = tableExtra
                    .stream()
                    .filter(basicDBObject -> {
                        //System.out.println(basicDBObject.get("_id") + " == " + extraSideOfNToNRelation.id);
                        return basicDBObject.get("_id").equals(extraSideOfNToNRelation.id);
                    })
                    .findAny()
                    .get();
            obj.put(newFieldInExtra, extraSideOfNToNRelation.references.toArray());
        }));
    }

	public static ArrayList<BasicDBObject> embed(ArrayList<BasicDBObject> toReceive,
                                                 ArrayList<BasicDBObject> toBeEmbedded,
                                                 String fieldInReceivingTable,
                                                 String fkNameInToBeEmbeddedTable){
        for(BasicDBObject objToReceive : toReceive){
            String id = (String) objToReceive.get("_id");
            ArrayList<BasicDBObject> listOfCities = new ArrayList<>();

            toBeEmbedded
                    .stream()
                    .filter((obj) -> obj.get(fkNameInToBeEmbeddedTable) != null)
                    .filter((obj) -> obj.get(fkNameInToBeEmbeddedTable).equals(id))
                    .forEach((obj) -> {
                        obj.remove(fkNameInToBeEmbeddedTable);
                        listOfCities.add(obj);
                    });
            objToReceive.put(fieldInReceivingTable, listOfCities.toArray());
        }

        return toReceive;
    }

	public static ArrayList<BasicDBObject> do1ToN(String targetTable) throws SQLException {
        return do1ToN(targetTable, new String[0]);
    }

	public static ArrayList<BasicDBObject> do1ToN(String targetTable, String[] fksToIgnore) throws SQLException {
        ArrayList<String> arrayFksToIgnore = new ArrayList<>();
        for(String aux : fksToIgnore){
            arrayFksToIgnore.add(aux.toUpperCase());
        }

        ArrayList<BasicDBObject> resultArray = new ArrayList<>();

        //fetches table info
        ResultSet rs = stmt.executeQuery("select * from " + targetTable);

        List<String> primaryKeys = getPrimaryKeys(targetTable);
        ArrayList<String> columnNames = getColumnNames(rs);
        ArrayList<ForeignKeyBundle> foreignKeyBundles = getForeignKeys(targetTable);

        //rs = stmt.executeQuery("select * from " + targetTable);

        ArrayList<String> allForeignKeys = new ArrayList<>();
        foreignKeyBundles.forEach(fkb -> fkb.fks.forEach(fk -> allForeignKeys.add(fk.fkVariable)));

        System.out.println("db.createCollection(\""+ targetTable +"\")");
        while(rs.next()){
            BasicDBObject obj = new BasicDBObject();

            //creating ID
            String id = makeId(rs, primaryKeys);
            obj.put("_id", id);

            //creating Foreign Keys

            foreignKeyBundles.stream().distinct().forEach(foreignKeyBundle -> {
                try {
                    if (!foreignKeyBundle.fks.get(0).pkConstraint.startsWith("UN")) {
                        String foreignKeyString = "";

                        if (arrayFksToIgnore.contains(foreignKeyBundle.fks.get(0).fkName.toUpperCase())) {
                            return;
                        }

                        for (ForeignKey foreignKey : foreignKeyBundle.fks) {
                            foreignKeyString += "_" + rs.getString(foreignKey.fkVariable);
                        }
                        obj.put(foreignKeyBundle.fks.get(0).fkName, foreignKeyString);
                    } else {
                        List<String> pks = getPrimaryKeys(foreignKeyBundle.fks.get(0).referencedTable);

                        String sql = "SELECT ";
                        for (String pk : pks) {
                            sql += pk + ", ";
                        }
                        sql = sql.substring(0, sql.length() - 2);
                        sql += " FROM " + foreignKeyBundle.fks.get(0).referencedTable + " WHERE ";
                        for (ForeignKey fk : foreignKeyBundle.fks) {
                            sql += fk.pkVariable + " = " + rs.getString(fk.fkVariable) + " AND ";
                        }
                        sql = sql.substring(0, sql.length() - 5);
                        //System.out.println(sql);

                        Statement stmt2 = conn.createStatement();
                        ResultSet rs2 = stmt2.executeQuery(sql);
                        rs2.next();
                        //System.out.println(makeId(rs2, pks));

                        obj.put(foreignKeyBundle.fks.get(0).fkName, makeId(rs2, pks));
                    }
                }catch(Exception e){
                    e.printStackTrace();
                }
            });

            //creating rest of variables
            for(String columnName : columnNames){
                if(!allForeignKeys.contains(columnName)){
                    obj.put(columnName, rs.getString(columnName));
                }
            }

            String bsonString = obj.toString();
            System.out.println(bsonString);

            resultArray.add(obj);
        }

        return resultArray;
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

        public boolean equals(Object obj){
            if(obj instanceof ArrayList) {
                ArrayList al = (ArrayList) obj;

                if(al.size() != fks.size()) return false;

                for(int i = 0; i < fks.size(); i++){
                    if(!fks.get(i).equals(al.get(i))){
                        return false;
                    }
                }
            }
            return true;
        }
    }
    public static class ForeignKey{
        public String fkName, referencedTable, fkVariable, pkVariable, pkConstraint;
    }
    private static ArrayList<ForeignKeyBundle> getForeignKeys(String tableName) throws SQLException {
        Statement stmt2 = conn.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT constraint_info.constraint_name FK_NAME, " +
                                         "       master_table.TABLE_NAME  PKTABLE_NAME, " +
                                         "       master_table.column_name PKCOLUMN_NAME, " +
                                         "       detail_table.column_name FKCOLUMN_NAME, " +
                                         "       master_table.CONSTRAINT_NAME PKCONSTRAINT" +
                                         "  FROM user_constraints  constraint_info, " +
                                         "       user_cons_columns detail_table, " +
                                         "       user_cons_columns master_table " +
                                         " WHERE constraint_info.constraint_name = detail_table.constraint_name " +
                                         "   AND constraint_info.r_constraint_name = master_table.constraint_name " +
                                         "   AND detail_table.POSITION = master_table.POSITION " +
                                         "   AND constraint_info.constraint_type = 'R' " +
                                         "   AND detail_table.TABLE_NAME = '" + tableName + "'");

        ArrayList<ForeignKey> foreignKeys = new ArrayList<>();
        while (rs.next()) {
            ForeignKey fk = new ForeignKey();
            fk.fkName = rs.getString("FK_NAME");
            fk.referencedTable = rs.getString("PKTABLE_NAME");
            fk.fkVariable = rs.getString("FKCOLUMN_NAME");
            fk.pkVariable = rs.getString("PKCOLUMN_NAME");
            fk.pkConstraint = rs.getString("PKCONSTRAINT");
            foreignKeys.add(fk);
        }
        stmt2.close();
        //foreignKeys.forEach((asd)-> System.out.println(asd.fkName));

        //gathering foreign keys into bundles
        ArrayList<ForeignKeyBundle> foreignKeyBundles = new ArrayList<>();
        foreignKeys.stream().map((ForeignKey fk) -> fk.fkName).forEach(fkName -> {
            ForeignKeyBundle fkb = new ForeignKeyBundle();
            foreignKeys.
                    stream().
                    filter(fkFiltered -> fkFiltered.fkName.equals(fkName)).
                    forEach(fkb.fks::add);
            //fkb.fks.stream().map((obj) -> obj.fkName).distinct().forEach(System.out::println);
            //System.out.println(fkName);
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
