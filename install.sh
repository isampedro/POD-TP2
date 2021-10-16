mvn clean install
cd server/target
tar -xzf *-bin.tar.gz
cd tpe2-g14-server-1.0-SNAPSHOT
chmod u+x run-*
cd ../../../client/target
tar -xzf *-bin.tar.gz
cd tpe2-g14-client-1.0-SNAPSHOT
chmod u+x run-*
cd ../../../