all: app_client

app_client: app_client.o
	$(LINK.c) -o $@ -Bstatic app_client.o -lm -lalocs -lrados

app_client.o: app_client.c
	$(COMPILE.c)  -g app_client.c

clean:
	$(RM) *.o app_client



