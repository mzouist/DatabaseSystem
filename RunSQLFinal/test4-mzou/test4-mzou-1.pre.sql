CREATE TABLE IF NOT EXISTS DTABLES (tname char(32), nodedriver char(64), nodeurl char(128), nodeuser char(16), nodepasswd char(16), partmtd int, nodeid int, partcol char(32), partparam1 char(32), partparam2 char(32));

INSERT INTO DTABLES (NODEDRIVER, NODEURL, NODEUSER, NODEPASSWD, NODEID) VALUES ('com.mysql.jdbc.Driver', 'jdbc:mysql://localhost:3306/mzou1', 'mzou', 'password', 1);

INSERT INTO DTABLES (TNAME, NODEDRIVER, NODEURL, NODEUSER, NODEPASSWD, NODEID) VALUES ('Books', 'com.mysql.jdbc.Driver', 'jdbc:mysql://localhost:3306/mzou2', 'mzou', 'password', 2);

INSERT INTO DTABLES (TNAME, NODEDRIVER, NODEURL, NODEUSER, NODEPASSWD, NODEID) VALUES ('Movies', 'com.mysql.jdbc.Driver', 'jdbc:mysql://localhost:3306/mzou3', 'mzou', 'password', 3);