import com.mongodb.BasicDBObject;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import java.io.*;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

public class TestMainClass {
    // JDBC driver name and database URL
    private static final String JDBC_DRIVER = "oracle.jdbc.driver.OracleDriver";

    public static String db_url = "jdbc:oracle:thin:@localhost:1521:xe";
    public static String user = "g8937204";
    public static String pass = "4703es";

    public static Connection conn = null;
    public static Statement stmt = null;

    public static final int CONNECTION_SUCCESS = 0;
    public static final int CONNECTION_ERROR_SQL = 1;
    public static final int CONNECTION_ERROR_FORNAME = 2;

    public static HashMap<String, ArrayList<BasicDBObject>> databaseMap;

    public static void main(String[] args) throws Exception {

        // TestMainClass 1ToN TableToBeProcessed;

        // TestMainClass NToN TableWithReferences
        //                    TableToReceiveExtraFields TableThatWillNotReceiveExtraFields
        //                    FkNameTowardsReceiver FkNameTowardsNonReceiver
        //                    NewFieldInReceiver NewFieldInExtra;

        // TestMainClass embed TableToReceive TableToBeEmbedded FieldInReceivingTable FkNameInTableToBeEmbedded;

        // TestMainClass remove TableToBeProcessed;

        startConnection(db_url, user, pass);
        databaseMap = new HashMap<String, ArrayList<BasicDBObject>>();

        if(args.length == 1 && args[0].equals("!")){
            callTest();
        }else if(args.length == 0){
            callFromInputStream(System.in);
            //callTest();
        }else{
            //callFromArguments(args);
        }

        closeConnection();

        writeTables("file.txt");

    }

    public static void callTest() throws Exception {
        String[] commands = {
                "1TON LE01ESTADO",
                "1TON LE02CIDADE",
                "1TON LE03ZONA",
                "1TON LE04BAIRRO",
                "1TON LE05URNA",
                "1TON LE06SESSAO",
                "1TON LE07PARTIDO",
                "1TON LE08CANDIDATO",
                "1TON LE09CARGO",
                "1TON LE10CANDIDATURA",
                "1TON LE11PLEITO",
                "1TON LE12PESQUISA",
                "1TON LE13INTENCAODEVOTO",

                "NTON LE11PLEITO LE06SESSAO LE10CANDIDATURA FKSESSAOCANDIDATURA FKCANDIDATURASESSAO Candidaturas Sessoes",
                "NTON LE13INTENCAODEVOTO LE12PESQUISA LE10CANDIDATURA FKPESQUISACANDIDATURA FKCANDIDATURAPESQUISA Candidaturas Pesquisas",

                "EMBED LE08CANDIDATO LE07PARTIDO Partido FKPARTIDOCANDIDATO",

                "REMOVE LE07PARTIDO",
                "REMOVE LE11PLEITO",
                "REMOVE LE13INTENCAODEVOTO",

                "INDEX"
        };

        for(String commandLine : commands){
            System.out.println(commandLine);

            String whatToDo = commandLine.split(" ")[0];

            switch (whatToDo) {
                case "1TON":
                    call1ToN(commandLine);
                    break;
                case "NTON":
                    callNToN(commandLine);
                    break;
                case "EMBED":
                    callEmbed(commandLine);
                    break;
                case "REMOVE":
                    callRemove(commandLine);
                    break;
                case "INDEX":
                    callIndex(commandLine);
                    break;
                case "EXIT":
                    return;
                default:
                    throw new Exception();
            }
        }
    }

    public static void callFromInputStream(InputStream is) throws Exception {
        Scanner sc = new Scanner(is);
        String commandLine;

        while (sc.hasNextLine()){
            commandLine = sc.nextLine();

            if (commandLine.toUpperCase().equals("EXIT")) return;

            if(commandLine.endsWith(";")) commandLine = commandLine.substring(0, commandLine.length() - 1);

            commandLine = commandLine.trim();

            if(commandLine.length() == 0) continue;

            String whatToDo = commandLine.split(" ")[0].toUpperCase();

            System.out.println(commandLine);

            try {
                switch (whatToDo) {
                    case "GO!":
                        callTest();
                        break;
                    case "1TON":
                        call1ToN(commandLine);
                        break;
                    case "NTON":
                        callNToN(commandLine);
                        break;
                    case "EMBED":
                        callEmbed(commandLine);
                        break;
                    case "REMOVE":
                        callRemove(commandLine);
                        break;
                    case "INDEX":
                        callIndex(commandLine);
                        break;
                    case "EXIT":
                        return;
                    case "HELP":
                        System.out.println(
                                " - AVAILABLE COMMANDS -\n" +
                        "1ToN TableToBeProcessed\n\n" +
                        "NToN TableWithReferences\n" +
                        "     TableToReceiveExtraFields TableThatWillNotReceiveExtraFields\n" +
                        "     FkNameTowardsReceiver FkNameTowardsNonReceiver\n" +
                        "     NewFieldInReceiver NewFieldInExtra\n\n" +
                        "EMBED TableToReceive TableToBeEmbedded FieldInReceivingTable FkNameInTableToBeEmbedded\n\n" +
                        "REMOVE TableToBeProcessed\n\n" +
                        "INDEX TableToBeIndexed\n\n" +
                        "EXIT"
                        );
                        break;
                    default:
                        System.out.println("ERROR! COMMAND NOT FOUND!");
                }
            }catch(ArrayIndexOutOfBoundsException aiobe){
                System.out.println("ERROR! TOO FEW PARAMETERS!");
            }
        }
    }

    public static void callFromArguments(String[] args) throws Exception {
        String commands = "";

        for(String arg : args){
            commands = commands.trim() + " " + arg.trim();
        }
        commands = commands.trim();

        String[] commandArray = commands.split(";");
        for(int i = 0; i < commandArray.length; i++){
            commandArray[i] = commandArray[i].trim().toUpperCase();
        }

        for (String commandLine : commandArray) {
            String whatToDo = commandLine.split(" ")[0];

            switch (whatToDo) {
                case "1TON":
                    call1ToN(commandLine);
                    break;
                case "NTON":
                    callNToN(commandLine);
                    break;
                case "EMBED":
                    callEmbed(commandLine);
                    break;
                case "REMOVE":
                    callRemove(commandLine);
                    break;
                case "EXIT":
                    return;
                default:
                    throw new Exception();
            }
        }
    }

    public static void call1ToN(String commandLine) throws SQLException {
        String table = commandLine.split(" ")[1];
        databaseMap.put(table, do1ToN(table));
    }

    public static void callNToN(String commandLine) throws SQLException {
        String[] commandSplitted = commandLine.split(" ");

        String table1 = commandSplitted[1];
        String table2 = commandSplitted[2];
        String table3 = commandSplitted[3];

        ensureTableExistenceOnMap(table1);
        ensureTableExistenceOnMap(table2);
        ensureTableExistenceOnMap(table3);

        createNToNRelations(databaseMap.get(commandSplitted[1]),
                            databaseMap.get(commandSplitted[2]),
                            databaseMap.get(commandSplitted[3]),
                            commandSplitted[4],
                            commandSplitted[5],
                            commandSplitted[6],
                            commandSplitted[7]);
    }

    public static void callEmbed(String commandLine) throws SQLException {
        String[] commandSplitted = commandLine.split(" ");

        String table1 = commandSplitted[1];
        String table2 = commandSplitted[2];

        ensureTableExistenceOnMap(table1);
        ensureTableExistenceOnMap(table2);

        databaseMap.replace(commandSplitted[1],
                embed(databaseMap.get(commandSplitted[1]),
                        databaseMap.get(commandSplitted[2]),
                        commandSplitted[3],
                        commandSplitted[4]));
    }

    public static void callRemove(String commandLine) throws SQLException {
        String table = commandLine.split(" ")[1];
        databaseMap.remove(table);
    }

    public static void callIndex(String commandLine) throws Exception {
        createAllIndexes();
    }

    public static void ensureTableExistenceOnMap(String table) throws SQLException {
        if(!databaseMap.containsKey(table)){
            databaseMap.put(table, do1ToN(table));
        }
    }

    public static void writeTables(String fileName) throws FileNotFoundException{
        File file = new File(fileName);
        file.delete();
        try(PrintStream ps = new PrintStream(file)) {
            databaseMap.forEach((name, value) -> {
                writeCommands(name, value, ps);
            });
        }
    }

    public static class Constraints{
        public char type;
        public ArrayList<String> fields;

        public Constraints(char type){
            this.type = type;
            fields = new ArrayList<>();
        }
    }

    public static void createAllIndexes() throws Exception{
        String query = "SELECT table_name FROM user_tables";

        ResultSet rs = stmt.executeQuery(query);
        ArrayList<String> tables = new ArrayList<>();

        while(rs.next()){
            tables.add(rs.getString("TABLE_NAME"));
        }

        File indexFile = new File("index.txt");
        indexFile.delete();
        try(PrintStream ps = new PrintStream(indexFile)){
            for (String table : tables) {
                createIndex(table, ps);
            }
        }
    }

    public static void createIndex(String table, PrintStream ps) throws SQLException, FileNotFoundException {

        String query = "SELECT CONS.CONSTRAINT_NAME, CONS.CONSTRAINT_TYPE, COLS.COLUMN_NAME, FK.TABLE_NAME, FK.COLUMN_NAME " +
        "FROM USER_CONSTRAINTS CONS " +
        "LEFT JOIN USER_CONS_COLUMNS COLS ON CONS.CONSTRAINT_NAME = COLS.CONSTRAINT_NAME " +
        "LEFT JOIN USER_CONS_COLUMNS FK ON FK.CONSTRAINT_NAME = CONS.R_CONSTRAINT_NAME AND FK.POSITION = COLS.POSITION " +
        "WHERE (CONSTRAINT_TYPE = 'P' OR CONSTRAINT_TYPE = 'R' OR CONSTRAINT_TYPE = 'U') AND COLS.TABLE_NAME = '" + table + "' " +
        "ORDER BY COLS.TABLE_NAME, COLS.COLUMN_NAME";

        ResultSet rs = stmt.executeQuery(query);
        HashMap<String, Constraints> constraintsHashMap = new HashMap<>();

        while (rs.next()){
            if(!constraintsHashMap.containsKey(rs.getString("CONSTRAINT_NAME"))){
                constraintsHashMap.put(rs.getString("CONSTRAINT_NAME"), new Constraints(rs.getString("CONSTRAINT_TYPE").charAt(0)));
            }

            Constraints constraints = constraintsHashMap.get(rs.getString("CONSTRAINT_NAME"));
            constraints.fields.add(rs.getString("COLUMN_NAME"));
        }


        constraintsHashMap.forEach((name, constraints) -> {
            if (constraints.type == 'U') {
                constraintsHashMap.forEach((internal_name, internal_constraints) -> {
                    if (internal_constraints.type == 'R' && internal_constraints.fields.equals(constraints)) {
                        constraints.fields.clear();
                        constraints.fields.add(internal_name);
                    }
                });
            }

            if(constraints.type == 'U' || constraints.type == 'P'){
                StringBuilder toPrint = new StringBuilder("db." + table + ".createIndex( {");
                constraints.fields.forEach((str) -> toPrint.append(" " + str + " : 1,"));
                String aux = toPrint.toString();

                aux = aux.substring(0, aux.length() - 1);
                aux += "} )";
                ps.println(aux);
            }
        });

    }

	public static void writeCommands(String tableName, ArrayList<BasicDBObject> data, PrintStream out){
        if(data == null) return;

        out.println("db.createCollection(\"" + tableName + "\")");
        data.forEach((obj) -> {
            out.println("db." + tableName + ".insert(" + obj.toJson(new JsonWriterSettings(JsonMode.SHELL)) + ")");
        });
        out.println();
    }

    public static class TwoStrings{
        String name;
        Object content;

        public TwoStrings(String name, Object content){
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
        public ArrayList<Object> references = new ArrayList<>();

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
                    twoStringsArrayList.add(new TwoStrings(name, value));
                }else if(name.equals(fkNameTowardsExtra)){
                    //receiver.references.add(new TwoStrings("_id", (String) value));
                    twoStringsArrayList.add(new TwoStrings("_id", value));
                }else if(name.equals(fkNameTowardsReceiver)){
                    extra.references.add(value);
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
            String id = (String) objToReceive.get(fkNameInToBeEmbeddedTable);
            ArrayList<BasicDBObject> listOfThingsToEmbed = new ArrayList<>();

            toBeEmbedded
                    .stream()
                    .filter((obj) -> obj.get("_id").equals(id))
                    .forEach((obj) -> {
                        BasicDBObject temp = (BasicDBObject)obj.copy();
                        temp.remove("_id");
                        listOfThingsToEmbed.add(temp);
                    });

            if(listOfThingsToEmbed.size() == 1){
                objToReceive.put(fieldInReceivingTable, listOfThingsToEmbed.get(0));
            }else if(listOfThingsToEmbed.size() > 1){
                objToReceive.put(fieldInReceivingTable, listOfThingsToEmbed.toArray());
            }

            objToReceive.remove(fkNameInToBeEmbeddedTable);

            /*
            toBeEmbedded
                    .stream()
                    .filter((obj) -> obj.get(fkNameInToBeEmbeddedTable) != null)
                    .filter((obj) -> obj.get(fkNameInToBeEmbeddedTable).equals(id))
                    .forEach((obj) -> {
                        obj.remove(fkNameInToBeEmbeddedTable);
                        listOfThingsToEmbed.add(obj);
                    });
            objToReceive.put(fieldInReceivingTable, listOfThingsToEmbed.toArray());
            */
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

        //System.out.println(targetTable);
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
                            String aux = rs.getString(foreignKey.fkVariable);
                            if(aux == null) return;

                            foreignKeyString += "_" + aux;
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
                String value = rs.getString(columnName);
                if(!allForeignKeys.contains(columnName) && value != null){
                    Date valueAsDate = toDate(value);

                    if(isInteger(value)){
                        obj.put(columnName, Integer.valueOf(value));
                    }else if(isNumber(value)) {
                        obj.put(columnName, Double.valueOf(value));
                    }else if(valueAsDate != null){
                        obj.put(columnName, valueAsDate);
                    }else{
                        obj.put(columnName, value);
                    }
                }
            }

            String bsonString = obj.toString();

            resultArray.add(obj);
        }

        return resultArray;
    }

    public static Date toDate(String value){
        //2016-03-05 00:00:00.0
        try {
            value = value.substring(0, value.lastIndexOf('.') - 1);
            SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            return parser.parse(value);
        } catch (Exception e) {
            return null;
        }
    }

    public static boolean isInteger(String num){
        try{
            Integer.parseInt(num);
            return true;
        }catch(Exception e){
            return false;
        }
    }

    public static boolean isNumber(String num){
        try{
            Double.parseDouble(num);
            return true;
        }catch(Exception e){
            return false;
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

            System.out.println("DONE!");

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
