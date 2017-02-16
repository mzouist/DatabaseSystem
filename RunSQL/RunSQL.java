
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class RunSQL {

    private static int nthreads = 0;
    private static String driver = null;
    private static String connUrl = null;
    private static String connUser = null;
    private static String connPwd = null;
    private static List<ClusterInfo> clusterList = new ArrayList<ClusterInfo>();

    public static void main(String[] args) throws InterruptedException, FileNotFoundException, IOException {
        RunSQL run = new RunSQL();
        run.updateCatalog("/Users/mzou/NetBeansProjects/RunSQL/src/clustercfg.cfg");
        run.getClusterNodes();

        Thread[] threads = new Thread[nthreads];
        for (int i = 0; i < nthreads; i++) {
            String driver = clusterList.get(i).getNodedriver();
            String connUrl = clusterList.get(i).getNodeurl();
            String connUser = clusterList.get(i).getNodeuser();
            String connPwd = clusterList.get(i).getNodepasswd();
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    Connection conn;
                    ResultSet resultSet;
                    String lines;
                    String status = null;
                    StringBuilder line = new StringBuilder();
                    String path = "/Users/mzou/NetBeansProjects/RunSQL/src/sqlfile.sql";
                    try {
                        Class.forName(driver).newInstance();
                        conn = DriverManager.getConnection(connUrl, connUser, connPwd);
                        BufferedReader br = new BufferedReader(new FileReader(path));

                        while ((lines = br.readLine()) != null) {
                            line.append(lines);
                        }
                        if (line.lastIndexOf(";") > -1) {
                            line.deleteCharAt(line.lastIndexOf(";"));
                        }
                        String query = line.toString();
                        PreparedStatement statement = conn
                                .prepareStatement(query);
                        resultSet = statement.executeQuery();
                        while (resultSet.next()) {
                            String ISBN = resultSet.getString("ISBN");
                            String Title = resultSet.getString("TITLE");
                            String Price = resultSet.getString("PRICE");
                            System.out.println(ISBN + " | " + Title + " | " + Price);
                        }
                        status = path + " success";
                    } catch (Exception e) {
                        status = path + "failed, " + e.getMessage();
                    } finally {
                        System.out.println("[" + connUrl + "]: " + status + ".");
                    }
                }
            });
            threads[i].start();
        }
    }

    public void getClusterNodes() {
        Connection conn;
        ResultSet resultSet;
        String nodeDriver, nodeUrl, nodeUser, nodePasswd;
        try {
            Class.forName(driver).newInstance();
            conn = DriverManager.getConnection(connUrl, connUser, connPwd);
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
            clusterList.remove(0);
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
                    conn = DriverManager.getConnection(catalogHostname + ";create=true", catalogUsername, catalogPasswd);
                    Statement stmt;
                    String query = "CREATE TABLE ICS421.DTABLES(tname char(32), \n"
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
                    pstmt = conn.prepareStatement("INSERT INTO ICS421.DTABLES (NODEDRIVER, NODEURL, NODEUSER, NODEPASSWD, TNAME) VALUES (?, ?, ?, ?, ?)");
                    pstmt.setString(1, catalogDriver);
                    pstmt.setString(2, catalogHostname);
                    pstmt.setString(3, catalogUsername);
                    pstmt.setString(4, catalogPasswd);
                    pstmt.setString(5, catalogTablename);
                    pstmt.executeUpdate();
                    status = "Catalog Node Updated";
                } catch (Exception e) {
                    status = "Catalog Node Update Failed, " + e.getMessage();
                } finally {
                    System.out.println("[" + catalogHostname + "]: " + status + ".");
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
                } catch (Exception e) {
                    status = "Cluster Node Update Failed, " + e.getMessage();
                } finally {
                    //System.out.println("[" + hostname + "]: " + status + ".");
                }
            }
        }
        try {
            if(!conn.isClosed())
                conn.close();
        } catch (SQLException e) {
            System.err.print(e.getMessage());
        }

    }

}
