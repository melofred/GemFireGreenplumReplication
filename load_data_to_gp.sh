GEMFIRE=/opt/gemfire
CURDIR=/home/pivotal/replication
java -classpath .:$CURDIR/replication.jar:$CURDIR/lib/postgresql-8.2-512.jdbc4.jar:$GEMFIRE/lib/gemfire.jar com.pivotal.gpdbreplication.in.ExternalTableToGemFire $1
