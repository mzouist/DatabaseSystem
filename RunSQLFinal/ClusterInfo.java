public class ClusterInfo {

        /*
       dtables(tname char(32), 
       nodedriver char(64), 
       nodeurl char(128), 
       nodeuser char(16), 
       nodepasswd char(16), 
       partmtd int, 
       nodeid int, 
       partcol char(32), 
       partparam1 char(32),
       partparam2 char(32))
         */
        private String tname;
        private String nodedriver;
        private String nodeurl;
        private String nodeuser;
        private String nodepasswd;
        private int partmtd;
        private int nodeid;
        private String partcol;
        private String partparam1;
        private String paraparam2;

        /**
         * @return the tname
         */
        public String getTname() {
            return tname;
        }

        /**
         * @param tname the tname to set
         */
        public void setTname(String tname) {
            this.tname = tname;
        }

        /**
         * @return the nodedriver
         */
        public String getNodedriver() {
            return nodedriver;
        }

        /**
         * @param nodedriver the nodedriver to set
         */
        public void setNodedriver(String nodedriver) {
            this.nodedriver = nodedriver;
        }

        /**
         * @return the nodeurl
         */
        public String getNodeurl() {
            return nodeurl;
        }

        /**
         * @param nodeurl the nodeurl to set
         */
        public void setNodeurl(String nodeurl) {
            this.nodeurl = nodeurl;
        }

        /**
         * @return the nodeuser
         */
        public String getNodeuser() {
            return nodeuser;
        }

        /**
         * @param nodeuser the nodeuser to set
         */
        public void setNodeuser(String nodeuser) {
            this.nodeuser = nodeuser;
        }

        /**
         * @return the nodepasswd
         */
        public String getNodepasswd() {
            return nodepasswd;
        }

        /**
         * @param nodepasswd the nodepasswd to set
         */
        public void setNodepasswd(String nodepasswd) {
            this.nodepasswd = nodepasswd;
        }

        /**
         * @return the partmtd
         */
        public int getPartmtd() {
            return partmtd;
        }

        /**
         * @param partmtd the partmtd to set
         */
        public void setPartmtd(int partmtd) {
            this.partmtd = partmtd;
        }

        /**
         * @return the nodeid
         */
        public int getNodeid() {
            return nodeid;
        }

        /**
         * @param nodeid the nodeid to set
         */
        public void setNodeid(int nodeid) {
            this.nodeid = nodeid;
        }

        /**
         * @return the partcol
         */
        public String getPartcol() {
            return partcol;
        }

        /**
         * @param partcol the partcol to set
         */
        public void setPartcol(String partcol) {
            this.partcol = partcol;
        }

        /**
         * @return the partparam1
         */
        public String getPartparam1() {
            return partparam1;
        }

        /**
         * @param partparam1 the partparam1 to set
         */
        public void setPartparam1(String partparam1) {
            this.partparam1 = partparam1;
        }

        /**
         * @return the paraparam2
         */
        public String getParaparam2() {
            return paraparam2;
        }

        /**
         * @param paraparam2 the paraparam2 to set
         */
        public void setParaparam2(String paraparam2) {
            this.paraparam2 = paraparam2;
        }

    }