
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RunSQL2 {

    private static int nthreads = 0;
    private static String driver = null;
    private static String connUrl = null;
    private static String connUser = null;
    private static String connPwd = null;
    private static String localdriver = null;
    private static String localconnUrl = null;
    private static String localconnUser = null;
    private static String localconnPwd = null;
    private static List<String> t1 = new ArrayList<String>();
    private static List<String> t2 = new ArrayList<String>();
    private static List<String> t3 = new ArrayList<String>();
    private static List<String> t4 = new ArrayList<String>();
    private static List<String> t5 = new ArrayList<String>();
    private static List<String> tt1, tt2, tt3, tt4, tt5;
    private static List<ClusterInfo> clusterList = new ArrayList<ClusterInfo>();
    private static Lock lock = new ReentrantLock();
    private static Condition condVar = lock.newCondition();

    
    /*
        RUNNING THIS PROGRAM REQUIRED TWO ARGUMENT PARAMATER
            1) CLUSTER CONFIGURATION FILE - WHICH INCLUDES
                A) CATALOG NODE
                B) LOCAL NODE
            2) SQL FILE
    */
    public static void main(String[] args) throws InterruptedException, FileNotFoundException, IOException {
        RunSQL2 r = new RunSQL2();
        r.ReadCatalog(args[0]);
        String lines;
        String mainQuery = "";
        List<String> tokens = new ArrayList<String>();
        List<String> select = new ArrayList<String>();
        List<String> from = new ArrayList<String>();
        List<String> where = new ArrayList<String>();
        StringBuilder line = new StringBuilder();
        List<String> colName = new ArrayList<String>();
        String path = args[1];
        tt1 = new ArrayList<String>();
        tt2 = new ArrayList<String>();
        tt3 = new ArrayList<String>();
        tt4 = new ArrayList<String>();
        tt5 = new ArrayList<String>();
        String tname;
        int tCount = 0;
        int ttCount = 0;
        BufferedReader br = new BufferedReader(new FileReader(path));
        while ((lines = br.readLine()) != null) {
            line.append(lines + " ");
        }
        if (line.lastIndexOf(";") > -1) {
            line.deleteCharAt(line.lastIndexOf(";"));
        }
        mainQuery = line.toString();
        StringTokenizer st = new StringTokenizer(line.toString(), " ");
        while (st.hasMoreTokens()) {
            tokens.add(st.nextToken());
        }
        for (int i = 0; i < tokens.size(); i++) {                 // SELECTED ATTRIBUTES
            if (tokens.get(i).equalsIgnoreCase("select")) {
                if (tokens.get(i + 1) != null) {
                    int j = i;
                    while (!tokens.get(j + 1).equalsIgnoreCase("from")) {
                        if (tokens.get(j + 1).substring(tokens.get(j + 1).length() - 1).equals(",")) {
                            StringBuilder sb = new StringBuilder(tokens.get(j + 1));
                            sb.deleteCharAt(tokens.get(j + 1).length() - 1);
                            select.add(sb.toString());
                        } else {
                            select.add(tokens.get(j + 1));
                        }
                        j++;
                    }
                }
            }
        }

        for (int i = 0; i < tokens.size(); i++) {                 // FROM TABLES
            if (tokens.get(i).equalsIgnoreCase("from")) {
                // TODO: SQL TABLE FROM CLAUSE FORMAT [TABLE S], OR [TABLE]
                if (tokens.get(i + 2) != null) {
                    int j = i;
                    while (!tokens.get(j + 1).equalsIgnoreCase("where")) {
                        if (tokens.get(j + 1).substring(tokens.get(j + 1).length() - 1).equals(",")) {
                            StringBuilder sb = new StringBuilder(tokens.get(j + 1));
                            sb.deleteCharAt(tokens.get(j + 1).length() - 1);
                            from.add(sb.toString());
                        } else {
                            from.add(tokens.get(j + 1));
                        }
                        j++;
                    }
                }
            }
        }

        try {
            String query = "";
            List<String> columnNames = new ArrayList<String>();
            Class.forName(driver).newInstance();
            Connection conn;
            Statement stmt = null;
            conn = DriverManager.getConnection(connUrl + "?verifyServerCertificate=false&useSSL=true", connUser, connPwd);
            br = new BufferedReader(new FileReader(path));
            query = "SELECT * FROM DTABLES";
            PreparedStatement statement = conn
                    .prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            while (rs.next()) {
                if (rs.getString("TNAME") != null) {
                    tname = rs.getString("TNAME").replaceAll("\\s+", "");
                    if (tname.equalsIgnoreCase(from.get(0))) {
                        // TODO: MIGHT NEED TO CHANGE IT FOR DYNAMIC LOAD
                        t1.add(tname);
                        t2.add(rs.getString("NODEDRIVER").replaceAll("\\s+", ""));
                        t3.add(rs.getString("NODEURL").replaceAll("\\s+", ""));
                        t4.add(rs.getString("NODEUSER").replaceAll("\\s+", ""));
                        t5.add(rs.getString("NODEPASSWD").replaceAll("\\s+", ""));
                        tCount++;
                    }
                    if (tname.equalsIgnoreCase(from.get(2))) {
                        // TODO: MIGHT NEED TO CHANGE IT FOR DYNAMIC LOAD
                        tt1.add(tname);
                        tt2.add(rs.getString("NODEDRIVER").replaceAll("\\s+", ""));
                        tt3.add(rs.getString("NODEURL").replaceAll("\\s+", ""));
                        tt4.add(rs.getString("NODEUSER").replaceAll("\\s+", ""));
                        tt5.add(rs.getString("NODEPASSWD").replaceAll("\\s+", ""));
                        ttCount++;
                    }
                }
            }
            conn.close();

            // GET ATTRIBUTES FROM THE FIRST TABLE IN ORDER TO CREATE TEMP TABLE
            Class.forName(t2.get(0)).newInstance();
            conn = DriverManager.getConnection(t3.get(0) + "?verifyServerCertificate=false&useSSL=true", t4.get(0), t5.get(0));
            query = "SELECT * FROM " + t1.get(0);
            //System.out.println(query);
            statement = conn
                    .prepareStatement(query);
            rs = statement.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();
            StringBuilder sb = new StringBuilder();
            StringBuilder sb1 = new StringBuilder();
            String name;
            String export;
            String tempQuery = "";
            // if it is the first node, create temp table
            for (int i = 1; i <= columnCount; i++) {
                colName.add(rsmd.getColumnName(i));
                name = rsmd.getColumnName(i);
                sb1.append(name + ", ");
                if (rsmd.getColumnType(i) == Types.INTEGER) {
                    sb.append(name + " INT, ");
                } else if (rsmd.getColumnType(i) == Types.VARCHAR) {
                    sb.append(name + " VARCHAR(128), ");
                } else if (rsmd.getColumnType(i) == Types.CHAR) {
                    sb.append(name + " CHAR(128), ");
                } else if (rsmd.getColumnType(i) == Types.VARCHAR) {
                    sb.append(name + " VARCHAR(128), ");
                } else if (rsmd.getColumnType(i) == Types.DATE) {
                    sb.append(name + " DATETIME, ");
                } else if (rsmd.getColumnType(i) == Types.DECIMAL) {
                    sb.append(name + " DECIMAL, ");
                }
            }
            sb1.setLength(sb1.length() - 2);
            sb.setLength(sb.length() - 2);
            tempQuery = sb.toString();
            export = sb1.toString();

            //System.out.println(sb.toString());
            conn.close();

            Class.forName(tt2.get(0)).newInstance();
            conn = DriverManager.getConnection(tt3.get(0) + "?verifyServerCertificate=false&useSSL=true", tt4.get(0), tt5.get(0));
            query = "SELECT * FROM " + tt1.get(0);
            //System.out.println(query);
            statement = conn
                    .prepareStatement(query);
            rs = statement.executeQuery();
            rsmd = rs.getMetaData();
            columnCount = rsmd.getColumnCount();
            StringBuilder sb2 = new StringBuilder();
            StringBuilder sb3 = new StringBuilder();
            String name1;
            String export1;
            String tempQuery1 = "";
            // if it is the first node, create temp table
            for (int i = 1; i <= columnCount; i++) {
                colName.add(rsmd.getColumnName(i));
                name1 = rsmd.getColumnName(i);
                sb3.append(name1 + ", ");
                if (rsmd.getColumnType(i) == Types.INTEGER) {
                    sb2.append(name1 + " INT, ");
                } else if (rsmd.getColumnType(i) == Types.VARCHAR) {
                    sb2.append(name1 + " VARCHAR(128), ");
                } else if (rsmd.getColumnType(i) == Types.CHAR) {
                    sb2.append(name1 + " CHAR(128), ");
                } else if (rsmd.getColumnType(i) == Types.VARCHAR) {
                    sb2.append(name1 + " VARCHAR(128), ");
                } else if (rsmd.getColumnType(i) == Types.DATE) {
                    sb2.append(name1 + " DATETIME, ");
                } else if (rsmd.getColumnType(i) == Types.DECIMAL) {
                    sb2.append(name1 + " DECIMAL, ");
                }
            }
            sb3.setLength(sb3.length() - 2);
            sb2.setLength(sb2.length() - 2);
            tempQuery1 = sb2.toString();
            export1 = sb3.toString();
            //System.out.println(tempQuery1);

            //System.out.println(sb.toString());
            conn.close();

            // FOR TESTING
            for (int x = 0; x < tt1.size(); x++) {
                //System.out.println(tt1.get(x));
            }

            // MAIN
            // CREATE TEMP TABLE FIRST AND JOIN TABLES
            // NOTE: CLOSING CONNECTION WILL DELETE TEMP TABLE
            Class.forName(localdriver).newInstance();
            // NEW CONNECTION (DO NOT USE CONN OR ELSE TEMP TABLE MIGHT DROP)
            final Connection iconn = DriverManager.getConnection(localconnUrl + "?verifyServerCertificate=false&useSSL=true", localconnUser, localconnPwd);
            query = "CREATE TEMPORARY TABLE " + t1.get(0) + " (" + tempQuery + ")"; // TEMP TABLE FOR TABLE 1
            //System.out.println(query);
            stmt = iconn.createStatement();
            stmt.executeUpdate(query);

            query = "CREATE TEMPORARY TABLE " + tt1.get(0) + " (" + tempQuery1 + ")"; // TEMP TABLE FOR TABLE 2
            //System.out.println(query);
            stmt = iconn.createStatement();
            stmt.executeUpdate(query);

            Thread thread1[] = new Thread[tCount];
            Thread thread2[] = new Thread[ttCount];

            for (int z = 0; z < tCount; z++) {
                final int c = z;
                final String d = export;
                // EXPORT ALL DATA TO TEMP TABLE
                thread1[z] = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        //System.out.println("Working on " + Thread.currentThread());
                        try {
                            // EXPORT CONNECTION 
                            Class.forName(t2.get(c)).newInstance();
                            Connection conn1 = DriverManager.getConnection(t3.get(c) + "?verifyServerCertificate=false&useSSL=true", t4.get(c), t5.get(c));

                            // GETTING DATA FROM TABLE 1
                            String exportQuery = "SELECT * FROM " + t1.get(c);
                            PreparedStatement statement1 = conn1.prepareStatement(exportQuery);
                            ResultSet rs = statement1.executeQuery();
                            ResultSetMetaData rsmd = rs.getMetaData();
                            List<String> columnNames = new ArrayList<String>();
                            Object temp = null;
                            int columnCount = rsmd.getColumnCount();
                            for (int i = 1; i <= columnCount; i++) {
                                columnNames.add(rsmd.getColumnName(i));
                            }
                            //System.out.println(sb.toString());
                            while (rs.next()) {
                                StringBuilder sb = new StringBuilder();
                                sb.append("INSERT INTO " + t1.get(0) + " (" + d + ") VALUES (");
                                for (int k = 0; k < columnCount; k++) {
                                    if (rsmd.getColumnType(k + 1) == Types.INTEGER) {
                                        temp = rs.getInt(k + 1);
                                        sb.append(temp + ", ");
                                    } else if (rsmd.getColumnType(k + 1) == Types.VARCHAR || rsmd.getColumnType(k + 1) == Types.CHAR) {
                                        temp = rs.getString(k + 1);
                                        sb.append("'" + temp + "', ");
                                    } else if (rsmd.getColumnType(k + 1) == Types.DECIMAL) {
                                        temp = rs.getFloat(k + 1);
                                        //System.out.println("temp " + temp);
                                        sb.append( temp + ", ");
                                    } else if (rsmd.getColumnType(k + 1) == Types.BOOLEAN) {
                                        temp = rs.getBoolean(k + 1);
                                        sb.append(temp + ", ");
                                    } else if (rsmd.getColumnType(k + 1) == Types.DATE) {
                                        temp = rs.getDate(k + 1);
                                        sb.append("'" + temp + "', ");
                                    }
                                    //sb.append(temp + ", ");
                                }
                                sb.setLength(sb.length() - 2);
                                sb.append(");");
                                //System.out.println(sb.toString());
                                String query2 = sb.toString();
                                //System.out.println(query2);
                                // NEED TO PERFORM A QUERY FOR EACH ROW
                                PreparedStatement pstmt;
                                pstmt = iconn.prepareStatement(query2);
                                pstmt.executeUpdate();
                            }

                        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException | SQLException e) {
                            System.err.println(e.getMessage());
                        } finally {
                            //System.out.println("Done with " + Thread.currentThread());
                        }

                    }
                });
                thread1[z].start();
            }

            // CREATE TEMP TABLE FOR SECOND TABLE
            for (int i = 0; i < ttCount; i++) {
                final int c = i;
                thread2[i] = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            //System.out.println("Working on " + Thread.currentThread());
                            // THIS PART WILL INSERT TABLE 2 DATA TO A NODE
                            Class.forName(tt2.get(c)).newInstance();
                            Connection conn1 = DriverManager.getConnection(tt3.get(c) + "?verifyServerCertificate=false&useSSL=true", tt4.get(c), tt5.get(c));
                            String exportQuery = "SELECT * FROM " + tt1.get(c);
                            PreparedStatement statement1 = conn1.prepareStatement(exportQuery);
                            ResultSet rs = statement1.executeQuery();
                            ResultSetMetaData rsmd = rs.getMetaData();
                            List<String> columnNames = new ArrayList<String>();
                            Object temp = null;
                            String name = null;
                            int columnCount = rsmd.getColumnCount();
                            StringBuilder sb1 = new StringBuilder();
                            for (int i = 1; i <= columnCount; i++) {
                                //System.out.println(rsmd.getColumnName(i));
                                columnNames.add(rsmd.getColumnName(i));
                                sb1.append(rsmd.getColumnName(i) + ", ");
                            }
                            sb1.setLength(sb1.length() - 2);
                            name = sb1.toString();
                            while (rs.next()) {
                                StringBuilder sb = new StringBuilder();
                                sb.append("INSERT INTO " + tt1.get(0) + " (" + name + ") VALUES (");
                                for (int k = 0; k < columnCount; k++) {
                                    if (rsmd.getColumnType(k + 1) == Types.INTEGER) {
                                        temp = rs.getInt(k + 1);
                                        sb.append(temp + ", ");
                                    } else if (rsmd.getColumnType(k + 1) == Types.VARCHAR || rsmd.getColumnType(k + 1) == Types.CHAR) {
                                        temp = rs.getString(k + 1);
                                        sb.append("'" + temp + "', ");
                                    } else if (rsmd.getColumnType(k + 1) == Types.DECIMAL) {
                                        temp = rs.getDouble(k + 1);
                                        sb.append("'" + temp + "', ");
                                    } else if (rsmd.getColumnType(k + 1) == Types.BOOLEAN) {
                                        temp = rs.getBoolean(k + 1);
                                        sb.append(temp + ", ");
                                    } else if (rsmd.getColumnType(k + 1) == Types.DATE) {
                                        temp = rs.getDate(k + 1);
                                        sb.append("'" + temp + "', ");
                                    }
                                    //sb.append(temp + ", ");
                                }
                                sb.setLength(sb.length() - 2);
                                sb.append(");");
                                String query2 = sb.toString();
                                //System.out.println(query2);

                                // NEED TO PERFORM A QUERY FOR EACH ROW
                                PreparedStatement pstmt;
                                pstmt = iconn.prepareStatement(query2);
                                pstmt.executeUpdate();
                            }

                        } catch (Exception e) {
                            System.err.println(e.getMessage());
                        } finally {
                            //System.out.println("Done with " + Thread.currentThread());
                        }
                    }
                });
                thread2[i].start();
            }

            //System.out.println(mainQuery);
            for (int z = 0; z < tCount; z++) {
                thread1[z].join();
            }

            for (int z = 0; z < ttCount; z++) {
                thread2[z].join();
            }

            // JOIN SQL PERFORMS HERE
            //System.out.println(mainQuery);
            statement = iconn
                    .prepareStatement(mainQuery);
            rs = statement.executeQuery();
            rsmd = rs.getMetaData();
            Object temp;
            String output;
            columnCount = rsmd.getColumnCount();
            //System.out.println(columnCount);
            double temp1;
            while (rs.next()) {
                StringBuilder sbb = new StringBuilder();
                for (int k = 0; k < columnCount; k++) {
                    if (rsmd.getColumnType(k + 1) == Types.INTEGER || rsmd.getColumnType(k + 1) == Types.BIGINT) {
                        temp = rs.getInt(k + 1);
                        sbb.append(temp + " | ");
                    } else if (rsmd.getColumnType(k + 1) == Types.VARCHAR || rsmd.getColumnType(k + 1) == Types.CHAR) {
                        temp = rs.getString(k + 1);
                        sbb.append(temp + " | ");
                    } else if (rsmd.getColumnType(k + 1) == Types.FLOAT || rsmd.getColumnType(k + 1) == Types.DECIMAL) {
                        temp1 = rs.getDouble(k + 1);
                        sbb.append(temp1 + " | ");
                    } else if (rsmd.getColumnType(k + 1) == Types.BOOLEAN) {
                        temp = rs.getBoolean(k + 1);
                        sbb.append(temp + " | ");
                    } else if (rsmd.getColumnType(k + 1) == Types.DATE) {
                        temp = rs.getDate(k + 1);
                        sbb.append(temp + " | ");
                    } else {
                        temp = rs.getString(k+1);
                        //System.out.println("asdfa" + rs.getString(k+1));
                        sbb.append(temp + " | ");
                    }
                }
                sbb.setLength(sbb.length() - 1);
                output = sbb.toString();
                System.out.println(output);
            }
            
            conn.close();
            iconn.close();
        } catch (IOException | ClassNotFoundException | IllegalAccessException | InstantiationException | SQLException e) {
            System.err.println(e.getMessage());
        }
    }

    public void ReadCatalog(String filePath) throws FileNotFoundException, IOException {
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        Connection conn = null;
        String line = null, driver = null, hostname = null, username = null, passwd = null, tname = null;
        String catalogDriver = null, catalogHostname = null, catalogUsername = null, catalogPasswd = null;
        String status;
        while ((line = br.readLine()) != null) {
            if (line.startsWith("catalog")) {
                status = null;
                line = line.substring(line.lastIndexOf('=') + 1).trim();
                catalogDriver = line;
                this.driver = catalogDriver;
                line = br.readLine();
                line = line.substring(line.lastIndexOf('=') + 1).trim();
                catalogHostname = line;
                this.connUrl = catalogHostname;
                //System.out.println(this.connUrl);
                line = br.readLine();
                line = line.substring(line.lastIndexOf('=') + 1).trim();
                catalogUsername = line;
                this.connUser = catalogUsername;
                line = br.readLine();
                line = line.substring(line.lastIndexOf('=') + 1).trim();
                catalogPasswd = line;
                this.connPwd = catalogPasswd;
            }
            if (line.startsWith("localnode")) {
                status = null;
                line = line.substring(line.lastIndexOf('=') + 1).trim();
                catalogDriver = line;
                this.localdriver = catalogDriver;
                line = br.readLine();
                line = line.substring(line.lastIndexOf('=') + 1).trim();
                catalogHostname = line;
                this.localconnUrl = catalogHostname;
                //System.out.println(this.connUrl);
                line = br.readLine();
                line = line.substring(line.lastIndexOf('=') + 1).trim();
                catalogUsername = line;
                this.localconnUser = catalogUsername;
                line = br.readLine();
                line = line.substring(line.lastIndexOf('=') + 1).trim();
                catalogPasswd = line;
                this.localconnPwd = catalogPasswd;
            }
        }
    }

}
