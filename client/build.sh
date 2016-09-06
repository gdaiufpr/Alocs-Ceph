rm -f app_client
rm -f app_client.o

gcc -o app_client  app_client.c -lm -lalocs -lrados  -ljvm -I/usr/lib/jvm/java-8-oracle/include -I/usr/lib/jvm/java-8-oracle/include/linux -L/usr/lib/jvm/java-8-oracle/jre/lib/amd64/server
