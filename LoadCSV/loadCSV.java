
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class loadCSV {

    private static int nthreads = 0;
    private static String driver = null;
    private static String connUrl = null;
    private static String connUser = null;
    private static String connPwd = null;
    private static List<ClusterInfo> clusterList = new ArrayList<ClusterInfo>();
    private static String partitionTable = null;
    private static String partitionMethod = null;
    private static String partitionCol = null;

    public static void main(String[] args) throws InterruptedException, FileNotFoundException, IOException {
        Connection conn;
        loadCSV run = new loadCSV();
        ResultSet resultSet;
        run.updateCatalog(args[0]);
        Thread[] thread = new Thread[nthreads];
        if (partitionMethod.compareToIgnoreCase("range") == 0) {
            int numThread = 0;
            try {
                Class.forName(driver).newInstance();
                conn = DriverManager.getConnection(connUrl + "?verifyServerCertificate=false&useSSL=true", connUser, connPwd);
                String query = "SELECT * FROM DTABLES WHERE PARTMTD = 1";
                PreparedStatement statement = conn
                        .prepareStatement(query);
                resultSet = statement.executeQuery();
                while (resultSet.next()) {
                    final String nodeDriver = resultSet.getString("NODEDRIVER").replaceAll("\\s+", "");
                    final String nodeUrl = resultSet.getString("NODEURL").replaceAll("\\s+", "");
                    final String nodeUser = resultSet.getString("NODEUSER").replaceAll("\\s+", "");
                    final String nodePwd = resultSet.getString("NODEPASSWD").replaceAll("\\s+", "");
                    final String tname = resultSet.getString("TNAME").replaceAll("\\s+", "");
                    final String partCol = resultSet.getString("PARTCOL").replaceAll("\\s+", "");
                    String param1 = resultSet.getString("PARTPARAM1");
                    param1 = param1.replaceAll("\\s+", "");
                    final int p1 = Integer.parseInt(param1);
                    String param2 = resultSet.getString("PARTPARAM2");
                    param2 = param2.replaceAll("\\s+", "");
                    final int p2 = Integer.parseInt(param2);
                    thread[numThread] = new Thread(new Runnable() {
                        @Override
                        public void run() {
                            int count = 0;
                            try {
                                Connection newConn;
                                Class.forName(nodeDriver).newInstance();
                                newConn = DriverManager.getConnection(nodeUrl + "?verifyServerCertificate=false&useSSL=true", nodeUser, nodePwd);
                                BufferedReader br = new BufferedReader(new FileReader(args[1]));
                                String line;
                                String query1 = "SELECT * From " + partitionTable;
                                PreparedStatement statement = newConn
                                        .prepareStatement(query1);
                                ResultSet resultSet = statement.executeQuery();
                                ResultSetMetaData rsmd = resultSet.getMetaData();
                                int compareCol = 0;
                                List<String> columnNames = new ArrayList<String>();
                                StringBuilder sqlCol = new StringBuilder();
                                int columnCount = rsmd.getColumnCount();
                                for (int i = 1; i <= columnCount; i++) {
                                    String name = rsmd.getColumnName(i);
                                    columnNames.add(name);
                                    sqlCol.append(name + ", ");
                                    if (name.compareTo(partitionCol) == 0) {
                                        compareCol = i - 1;
                                    }
                                }
                                sqlCol.setLength(sqlCol.length() - 2);
                                System.out.println("[" + nodeUrl + "]: inserting data.");
                                while ((line = br.readLine()) != null) {
                                    String query;
                                    List<String> stringAry = new ArrayList<String>();
                                    String[] dataArray = line.split("\\|");
                                    StringBuilder valueCol = new StringBuilder();
                                    for (int i = 0; i < dataArray.length; i++) {
                                        stringAry.add(dataArray[i]);
                                        if (rsmd.getColumnType(i + 1) == Types.CHAR) {
                                            valueCol.append("'" + dataArray[i] + "', ");
                                        } else if (rsmd.getColumnType(i + 1) == Types.DATE) {
                                            valueCol.append("'" + dataArray[i] + "', ");
                                        } else if (rsmd.getColumnType(i + 1) == Types.VARCHAR) {
                                            valueCol.append("'" + dataArray[i] + "', ");
                                        } else {
                                            valueCol.append(dataArray[i] + ", ");
                                        }
                                    }
                                    valueCol.setLength(valueCol.length() - 2);
                                    if (stringAry.size() == columnNames.size()) {
                                        if (Integer.parseInt(stringAry.get(compareCol)) >= p1
                                                && Integer.parseInt(stringAry.get(compareCol)) < p2) {
                                            query = "INSERT INTO " + partitionTable + "(" + sqlCol.toString() + ") VALUES ("
                                                    + valueCol.toString() + ")";
                                            count++;
                                            Statement stmt = newConn.createStatement();
                                            stmt.executeUpdate(query);
                                        }
                                    } else {
                                        System.err.println("Column size doesn't match with Data size");
                                    }
                                }
                                resultSet.close();
                                statement.close();
                                newConn.close();
                            } catch (Exception e) {
                                System.err.println(e.getMessage());
                            } finally {
                                System.out.println("[" + nodeUrl + "]: " + count + " rows inserted.");
                            }
                        }
                    });
                    thread[numThread].start();
                    numThread++;
                }
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        } else if (partitionMethod.compareToIgnoreCase("hash") == 0) {
            int numThread = 0;
            try {
                Class.forName(driver).newInstance();
                conn = DriverManager.getConnection(connUrl + "?verifyServerCertificate=false&useSSL=true", connUser, connPwd);
                String count = "SELECT COUNT(*) FROM DTABLES WHERE PARTMTD = 2";
                PreparedStatement countStmt = conn
                        .prepareStatement(count);
                ResultSet countResult = countStmt.executeQuery();
                while (countResult.next()) {
                    nthreads = countResult.getInt(1);
                }
                Thread[] thread2 = new Thread[nthreads];
                countResult.close();
                String query = "SELECT * FROM DTABLES WHERE PARTMTD = 2";
                PreparedStatement statement = conn
                        .prepareStatement(query);
                resultSet = statement.executeQuery();
                while (resultSet.next()) {
                    final String nodeDriver = resultSet.getString("NODEDRIVER").replaceAll("\\s+", "");
                    final String nodeUrl = resultSet.getString("NODEURL").replaceAll("\\s+", "");
                    final String nodeUser = resultSet.getString("NODEUSER").replaceAll("\\s+", "");
                    final String nodePwd = resultSet.getString("NODEPASSWD").replaceAll("\\s+", "");
                    final String tname = resultSet.getString("TNAME").replaceAll("\\s+", "");
                    final String partCol = resultSet.getString("PARTCOL").replaceAll("\\s+", "");
                    final int nodeId = resultSet.getInt("NODEID");
                    String param1 = resultSet.getString("PARTPARAM1");
                    param1 = param1.replaceAll("\\s+", "");
                    final int p1 = Integer.parseInt(param1);
                    thread2[numThread] = new Thread(new Runnable() {
                        @Override
                        public void run() {
                            int insertedData = 0;
                            try {
                                Connection newConn;
                                Class.forName(nodeDriver).newInstance();
                                newConn = DriverManager.getConnection(nodeUrl + "?verifyServerCertificate=false&useSSL=true", nodeUser, nodePwd);
                                BufferedReader br = new BufferedReader(new FileReader(args[1]));
                                String line;
                                String query1 = "SELECT * From " + partitionTable;
                                PreparedStatement statement = newConn
                                        .prepareStatement(query1);
                                ResultSet resultSet = statement.executeQuery();
                                ResultSetMetaData rsmd = resultSet.getMetaData();
                                int compareCol = 0;
                                List<String> columnNames = new ArrayList<String>();
                                StringBuilder sqlCol = new StringBuilder();
                                int columnCount = rsmd.getColumnCount();
                                for (int i = 1; i <= columnCount; i++) {
                                    String name = rsmd.getColumnName(i);
                                    columnNames.add(name);
                                    sqlCol.append(name + ", ");
                                    if (name.compareTo(partitionCol) == 0) {
                                        compareCol = i - 1;
                                    }
                                }
                                sqlCol.setLength(sqlCol.length() - 2);
                                System.out.println("[" + nodeUrl + "]: inserting data.");
                                while ((line = br.readLine()) != null) {
                                    String query;
                                    List<String> stringAry = new ArrayList<String>();
                                    String[] dataArray = line.split("\\|");
                                    StringBuilder valueCol = new StringBuilder();
                                    for (int i = 0; i < dataArray.length; i++) {
                                        stringAry.add(dataArray[i]);
                                        if (rsmd.getColumnType(i + 1) == Types.CHAR) {
                                            valueCol.append("'" + dataArray[i] + "', ");
                                        } else if (rsmd.getColumnType(i + 1) == Types.DATE) {
                                            valueCol.append("'" + dataArray[i] + "', ");
                                        } else if (rsmd.getColumnType(i + 1) == Types.VARCHAR) {
                                            valueCol.append("'" + dataArray[i] + "', ");
                                        } else {
                                            valueCol.append(dataArray[i] + ", ");
                                        }
                                    }
                                    valueCol.setLength(valueCol.length() - 2);
                                    if (stringAry.size() == columnNames.size()) {
                                        int temp = Integer.parseInt(stringAry.get(compareCol)) % p1;
                                        if ((nodeId - 1) == (temp + 1)) {
                                            query = "INSERT INTO " + partitionTable + "(" + sqlCol.toString() + ") VALUES ("
                                                    + valueCol.toString() + ")";
                                            insertedData++;
                                            //System.out.println(query);
                                            //System.out.println(nodeId + ": " + "order_key: " + dataArray[0]);
                                            Statement stmt = newConn.createStatement();
                                            stmt.executeUpdate(query);
                                        }
                                    } else {
                                        System.err.println("Column size doesn't match with Data size");
                                    }
                                }
                                statement.close();
                                newConn.close();
                            } catch (IOException | ClassNotFoundException | IllegalAccessException | InstantiationException | NumberFormatException | SQLException e) {
                                System.err.println(e.getMessage());
                            } finally {
                                System.out.println("[" + nodeUrl + "]: " + insertedData + " rows inserted.");
                            }
                        }
                    });
                    thread2[numThread].start();
                    numThread++;
                }
            } catch (ClassNotFoundException | IllegalAccessException | InstantiationException | NumberFormatException | SQLException e) {
                System.err.println(e.getMessage());
            }
        }

    }

    public void getClusterNodes() {
        Connection conn;
        ResultSet resultSet;
        String nodeDriver, nodeUrl, nodeUser, nodePasswd;
        try {
            Class.forName(driver).newInstance();
            conn = DriverManager.getConnection(connUrl + "?verifyServerCertificate=false&useSSL=true", connUser, connPwd);
            Statement stmt;
            String query = "SELECT * FROM DTABLES";
            PreparedStatement statement = conn
                    .prepareStatement(query);
            resultSet = statement.executeQuery();
            while (resultSet.next()) {
                ClusterInfo cluster = new ClusterInfo();
                nodeDriver = resultSet.getString("NODEDRIVER");
                nodeDriver = nodeDriver.replaceAll("\\s+", "");
                cluster.setNodedriver(nodeDriver);
                nodeUrl = resultSet.getString("NODEURL");
                nodeUrl = nodeUrl.replaceAll("\\s+", "");
                cluster.setNodeurl(nodeUrl);
                nodeUser = resultSet.getString("NODEUSER");
                nodeUser = nodeUser.replaceAll("\\s+", "");
                cluster.setNodeuser(nodeUser);
                nodePasswd = resultSet.getString("NODEPASSWD");
                nodePasswd = nodePasswd.replaceAll("\\s+", "");
                cluster.setNodepasswd(nodePasswd);
                clusterList.add(cluster);
            }
            clusterList.remove(0); //Catalog data is in clusterList.get(0)
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public void updateCatalog(String filePath) throws FileNotFoundException, IOException {
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        Connection conn = null;
        String line = null, driver = null, hostname = null, username = null, passwd = null, tname = null;
        String catalogDriver = null, catalogHostname = null, catalogUsername = null, catalogPasswd = null, catalogTablename = null;
        String status;
        int count = 0;
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
                catalogTablename = catalogHostname.substring(line.lastIndexOf('/') + 1).trim();
                line = br.readLine();
                line = line.substring(line.lastIndexOf('=') + 1).trim();
                catalogUsername = line;
                this.connUser = catalogUsername;
                line = br.readLine();
                line = line.substring(line.lastIndexOf('=') + 1).trim();
                catalogPasswd = line;
                this.connPwd = catalogPasswd;
                try {
                    Class.forName(catalogDriver).newInstance();
                    conn = DriverManager.getConnection(catalogHostname + "?verifyServerCertificate=false&useSSL=true", catalogUsername, catalogPasswd);
                    Statement stmt;
                    String query = "CREATE TABLE IF NOT EXISTS DTABLES(tname char(32), \n"
                            + "   nodedriver char(64), \n"
                            + "   nodeurl char(128), \n"
                            + "   nodeuser char(16), \n"
                            + "   nodepasswd char(16), \n"
                            + "   partmtd int, \n"
                            + "   nodeid int NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 0, INCREMENT BY 1), \n"
                            + "   partcol char(32), \n"
                            + "   partparam1 char(32),\n"
                            + "   partparam2 char(32))";
                    stmt = conn.createStatement();
                    stmt.executeUpdate(query);
                    PreparedStatement pstmt;
                    pstmt = conn.prepareStatement("INSERT INTO ICS421.DTABLES (NODEDRIVER, NODEURL, NODEUSER, NODEPASSWD) VALUES (?, ?, ?, ?)");
                    pstmt.setString(1, catalogDriver);
                    pstmt.setString(2, catalogHostname);
                    pstmt.setString(3, catalogUsername);
                    pstmt.setString(4, catalogPasswd);
                    pstmt.executeUpdate();
                    conn.close();
                    status = "Catalog Node Updated";
                } catch (Exception e) {
                    status = "Catalog Node Update Failed, " + e.getMessage();
                } finally {
                    //System.out.println("[" + catalogHostname + "]: " + status + ".");
                }
            }

            if (line.startsWith("numnodes")) {
                line = line.substring(line.lastIndexOf('=') + 1).trim();
                nthreads = Integer.parseInt(line);
            }

            if (line.startsWith("node")) {
                status = null;
                boolean existed = false;
                line = line.substring(line.lastIndexOf('=') + 1).trim();
                driver = line;
                line = br.readLine();
                line = line.substring(line.lastIndexOf('=') + 1).trim();
                hostname = line;
                tname = hostname.substring(line.lastIndexOf('/') + 1).trim();
                line = br.readLine();
                line = line.substring(line.lastIndexOf('=') + 1).trim();
                username = line;
                line = br.readLine();
                line = line.substring(line.lastIndexOf('=') + 1).trim();
                passwd = line;
                try {
                    Class.forName(catalogDriver).newInstance();
                    conn = DriverManager.getConnection(catalogHostname + "?verifyServerCertificate=false&useSSL=true", catalogUsername, catalogPasswd);
                    ResultSet resultSet;
                    String query = "SELECT NODEURL FROM DTABLES";
                    PreparedStatement statement = conn
                            .prepareStatement(query);
                    List<String> list = new ArrayList<String>();
                    resultSet = statement.executeQuery();
                    while (resultSet.next()) {
                        String url = resultSet.getString("NODEURL");
                        url = url.replaceAll("\\s+", "");
                        list.add(url);
                    }
                    for (int i = 0; i < list.size(); i++) {
                        if (list.get(i).toString().compareToIgnoreCase(hostname) == 0) {
                            existed = true;
                        }
                    }
                    if (existed == false) {
                        PreparedStatement pstmt;
                        pstmt = conn.prepareStatement("INSERT INTO ICS421.DTABLES (NODEDRIVER, NODEURL, NODEUSER, NODEPASSWD, TNAME) VALUES (?, ?, ?, ?, ?)");
                        pstmt.setString(1, driver);
                        pstmt.setString(2, hostname);
                        pstmt.setString(3, username);
                        pstmt.setString(4, passwd);
                        pstmt.setString(5, tname);
                        pstmt.executeUpdate();
                        status = "Cluster Node Added";
                    } else {
                        status = "Cluster Node Update Failed";
                    }
                    conn.close();
                } catch (Exception e) {
                    status = "Cluster Node Update Failed, " + e.getMessage();
                }
            } // End "node"
            if (line.startsWith("tablename")) {
                line = line.substring(line.lastIndexOf('=') + 1).trim();
                partitionTable = line;
                try {
                    Class.forName(catalogDriver).newInstance();
                    conn = DriverManager.getConnection(catalogHostname + "?verifyServerCertificate=false&useSSL=true", catalogUsername, catalogPasswd);
                    String query = "UPDATE DTABLES SET TNAME = '" + partitionTable + "' WHERE NODEURL != '" + catalogHostname + "'";
                    Statement stmt;
                    stmt = conn.createStatement();
                    stmt.executeUpdate(query);
                    conn.close();
                } catch (SQLException e) {
                    System.err.println(e.getMessage());
                } catch (Exception e) {
                    System.err.println(e.getMessage());
                }
            }
            if (line.startsWith("partition")) {
                String type = line;
                type = type.substring(type.indexOf(".") + 1);
                type = type.substring(0, type.indexOf("="));
                if (type.compareToIgnoreCase("method") == 0) {
                    line = line.substring(line.lastIndexOf('=') + 1).trim();
                    try {
                        Class.forName(catalogDriver).newInstance();
                        conn = DriverManager.getConnection(catalogHostname + "?verifyServerCertificate=false&useSSL=true", catalogUsername, catalogPasswd);
                        String query;
                        if (line.compareToIgnoreCase("range") == 0) {
                            query = "UPDATE DTABLES SET PARTMTD = 1 WHERE NODEURL != '" + catalogHostname + "'";
                            partitionMethod = "range";
                        } else if (line.compareToIgnoreCase("hash") == 0) {
                            query = "UPDATE DTABLES SET PARTMTD = 2 WHERE NODEURL != '" + catalogHostname + "'";
                            partitionMethod = "hash";
                        } else {
                            query = "UPDATE DTABLES SET PARTMTD = 0 WHERE NODEURL != '" + catalogHostname + "'";
                            partitionMethod = "none";
                        }
                        Statement stmt;
                        stmt = conn.createStatement();
                        stmt.executeUpdate(query);
                        conn.close();
                    } catch (SQLException e) {
                        System.err.println(e.getMessage());
                    } catch (Exception e) {
                        System.err.println(e.getMessage());
                    } finally {

                    }

                } else if (type.compareToIgnoreCase("column") == 0) {
                    line = line.substring(line.lastIndexOf('=') + 1).trim();
                    try {
                        Class.forName(catalogDriver).newInstance();
                        conn = DriverManager.getConnection(catalogHostname + "?verifyServerCertificate=false&useSSL=true", catalogUsername, catalogPasswd);
                        String query = "UPDATE DTABLES SET PARTCOL = '" + line + "' WHERE NODEURL != '" + catalogHostname + "'";
                        partitionCol = line;
                        Statement stmt;
                        stmt = conn.createStatement();
                        stmt.executeUpdate(query);
                        conn.close();
                    } catch (SQLException e) {
                        System.err.println(e.getMessage());
                    } catch (Exception e) {
                        System.err.println(e.getMessage());
                    }
                } else if (type.startsWith("node", 0)) { // This is for range cluster input
                    if (partitionMethod.compareToIgnoreCase("range") == 0) {
                        String node = line;
                        String param = line;
                        node = node.substring(node.indexOf(".") + 1);
                        node = node.substring(4, node.indexOf("."));
                        param = param.substring(param.lastIndexOf(".") + 1);
                        param = param.substring(5, param.indexOf("="));
                        String parmaCnt = line.substring(line.lastIndexOf('=') + 1).trim();

                        String query = "UPDATE DTABLES SET PARTPARAM" + param + " = '" + parmaCnt + "' WHERE NODEID = " + node;
                        try {
                            Class.forName(catalogDriver).newInstance();
                            conn = DriverManager.getConnection(catalogHostname + "?verifyServerCertificate=false&useSSL=true", catalogUsername, catalogPasswd);
                            Statement stmt;
                            stmt = conn.createStatement();
                            stmt.executeUpdate(query);
                            conn.close();
                        } catch (SQLException e) {
                            System.err.println(e.getMessage());
                        } catch (Exception e) {
                            System.err.println(e.getMessage());
                        }
                    }
                } else if (type.startsWith("param", 0)) { // This is for hash cluster input
                    if (partitionMethod.compareToIgnoreCase("hash") == 0) {
                        String param = line;
                        param = param.substring(param.lastIndexOf(".") + 1);
                        param = param.substring(5, param.indexOf("="));
                        String parmaCnt = line.substring(line.lastIndexOf('=') + 1).trim();
                        String query = "UPDATE DTABLES SET PARTPARAM" + param + " = '" + parmaCnt + "' WHERE NODEID != 0";
                        try {
                            Class.forName(catalogDriver).newInstance();
                            conn = DriverManager.getConnection(catalogHostname + "?verifyServerCertificate=false&useSSL=true", catalogUsername, catalogPasswd);
                            Statement stmt;
                            stmt = conn.createStatement();
                            stmt.executeUpdate(query);
                            query = "UPDATE DTABLES SET PARTPARAM2 = null WHERE NODEID != 0";
                            stmt.executeUpdate(query);
                            conn.close();
                        } catch (SQLException e) {
                            System.err.println(e.getMessage());
                        } catch (Exception e) {
                            System.err.println(e.getMessage());
                        }
                    }
                }
            }
        } // End while 

    }

}
