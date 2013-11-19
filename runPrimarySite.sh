GEMFIRE=/opt/gemfire
CURDIR=`pwd`
java -classpath .:$CURDIR/replication.jar:$CURDIR/lib/postgresql-8.2-512.jdbc4.jar:$GEMFIRE/lib/gemfire.jar:$GEMFIRE/lib/antlr.jar:$CURDIR/lib/commons-logging-1.1.1.jar:$CURDIR/lib/spring-core-3.1.4.RELEASE.jar:$CURDIR/lib/spring-beans-3.1.4.RELEASE.jar:$CURDIR/lib/spring-context-3.1.4.RELEASE.jar:$CURDIR/lib/spring-asm-3.1.4.RELEASE.jar:$CURDIR/lib/spring-expression-3.1.4.RELEASE.jar:$CURDIR/lib/commons-io-2.4.jar:$CURDIR/lib/aopalliance-1.0.jar:$CURDIR/lib/spring-aop-3.1.4.RELEASE.jar com.pivotal.gpdbreplication.GPReplicationPrimarySite
