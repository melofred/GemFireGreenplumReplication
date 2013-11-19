CURDIR=`pwd`
gfsh start server --name=server1 --server-bind-address=mdw --cache-xml-file=../site1.xml --properties-file=../site1.properties --dir=server1 --group=group1 --classpath=.:$CURDIR/lib/postgresql-8.2-512.jdbc4.jar:$CURDIR/replication.jar:$CURDIR/lib/snappy-java-1.1.0.1.jar::$CURDIR/lib/spring-core-3.1.4.RELEASE.jar:$CURDIR/lib/spring-beans-3.1.4.RELEASE.jar:$CURDIR/lib/spring-context-3.1.4.RELEASE.jar:$CURDIR/lib/spring-asm-3.1.4.RELEASE.jar:$CURDIR/lib/spring-expression-3.1.4.RELEASE.jar:$CURDIR/lib/commons-io-2.4.jar:$CURDIR/lib/aopalliance-1.0.jar:$CURDIR/lib/spring-aop-3.1.4.RELEASE.jar --J=-Dorg.xerial.snappy.lib.name=libsnappyjava.so --J=-Dorg.xerial.snappy.lib.path=/home/pivotal/replication/snappy --J=-Xmx2g


