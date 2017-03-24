#! /bin/bash

# CLEAN UP .CLASS FILES
rm *.class

# ONE PASSWORD FOR ALL
read -s -p "MySQL Password: " password

# TEST-1 FOR RUNDDL
printf "\n\nRunning testing for RunDDl\n"
. ./test1-mzou/test1-mzou-1.pre $password
. compile.sh
. run.sh ./test1-mzou/test1-mzou-1.cfg ./test1-mzou/ddlfile.sql | sort > ./test1-mzou/test1-mzou-1.out
diff ./test1-mzou/test1-mzou-1.out ./test1-mzou/test1-mzou-1.post.exp
. ./test1-mzou/test1-mzou-1.post $password | sort > ./test1-mzou/test1-mzou-2.out
diff ./test1-mzou/test1-mzou-2.out ./test1-mzou/test1-mzou-2.post.exp

rm *.class
# TEST-1 FOR RUNSQL
printf "\nRunning testing for RunSQL\n"
. ./test2-mzou/test2-mzou-1.pre $password
. compile.sh
. run.sh ./test2-mzou/test2-mzou-1.cfg ./test2-mzou/test2-mzou-sqlfile.sql | sort > ./test2-mzou/test2-mzou-1.out
diff ./test2-mzou/test2-mzou-1.out ./test2-mzou/test2-mzou-1.post.exp

rm *.class
# TEST-1 FOR LOADCSV
printf "\nRunning testing for LoadCSV\n"
. ./test3-mzou/test3-mzou-1.pre $password
. compile.sh
. run.sh ./test3-mzou/test3-mzou-1.cfg ./test3-mzou/test3-mzou-1.csv | sort > ./test3-mzou/test3-mzou-1.out
diff ./test3-mzou/test3-mzou-1.out ./test3-mzou/test3-mzou-1.post.exp
. ./test3-mzou/test3-mzou-1.post $password | sort > ./test3-mzou/test3-mzou-2.out
diff ./test3-mzou/test3-mzou-2.out ./test3-mzou/test3-mzou-2.post.exp


rm *.class
# TEST-1 FOR RUNSQL2
printf "\nRunning testing for RunSQL2\n"
. ./test4-mzou/test4-mzou-1.pre $password
. compile.sh
. run.sh ./test4-mzou/test4-mzou-1.cfg ./test4-mzou/test4-mzou-1.sqlfile.sql | sort > ./test4-mzou/test4-mzou-1.out
diff ./test4-mzou/test4-mzou-1.post.exp ./test4-mzou/test4-mzou-1.out

rm *.class
printf "\nTesting Script Terminated.\n"





