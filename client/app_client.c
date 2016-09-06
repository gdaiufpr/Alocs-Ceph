#include <stdlib.h>
#include <stdio.h>
#include <alocs/libalocs.h>

void app_create_server();
void app_create_dir();
void app_drop_dir();
void app_create_bucket();
void app_drop_bucket();
void app_put_pair();
void app_get_pair();
void app_remove_pair();

int retOper;
char idBucket[30],dirName[30],srvName[30];
char value[30];
uint64_t initKey,finKey,key;
PAIR_T pair;

void app_create_server(){
	printf("Create Server\n");
	
	printf("servidor: ");scanf("%s",srvName);
	
	retOper = create_server(srvName);

	if(retOper == 1)
		fprintf(stdout,"[app_create_dir/app_client.c] O Servidor não foi criado!\n");
	
	
}

void app_create_dir(){
	printf("Create Dir\n");
	
	printf("servidor: ");scanf("%s",srvName);
	printf("diretório: ");scanf("%s",dirName);
	
	retOper = create_dir(dirName,srvName);
	if(retOper == 1)
		fprintf(stdout,"[app_create_dir/app_client.c] O Diretório não foi criado!\n");
	
}

void app_drop_dir(){
	printf("DROP Dir\n");
	
	printf("servidor: ");scanf("%s",srvName);
	printf("diretório: ");scanf("%s",dirName);
	
	retOper = ss_drop_dir(dirName,srvName);
	if(retOper == 1)
		fprintf(stdout,"[app_drop_dir/app_client.c] O Diretório não foi removido!\n");
}

void app_create_bucket(){
	
	printf("Create Bucket\n");

	printf("servidor: ");scanf("%s",srvName);
	printf("diretorio: ");scanf("%s",dirName);
	printf("id Bucket: ");scanf("%s",idBucket);
	printf("chave inicial: ");scanf("%lu",&initKey);
	printf("chave final: ");scanf("%lu",&finKey);
	
	retOper = create_bucket(srvName, dirName,idBucket,initKey,finKey);
	if(retOper == 1)
		fprintf(stdout,"[app_create_bucket/app_client.c] O Bucket não foi criado!\n");
}

void app_drop_bucket(){
	
	printf("DROP Bucket\n");
	printf("idBucket: ");scanf("%s",idBucket);
		
	drop_bucket(idBucket);
}

void app_put_pair(){
	
	printf("PUT Pair\n");
	printf("chave: ");scanf("%lu",&key);
	printf("valor: ");scanf("%s",value);
	
	put_pair(key,value);
}

void app_get_pair(){
	
	printf("GET Pair\n");
	printf("chave: ");scanf("%lu",&key);
			
	retOper = get_pair(key,&pair);
	
	if(retOper == 0){
		printf("pair:%s\n",pair);
		free(pair);
	}else 
		printf("Chave não encontrada!\n");
  
	
}

void app_remove_pair(){
	
	printf("Remove Pair\n");
	printf("chave: ");scanf("%lu",&key);
			
	rem_pair(key);	
}

int main () {

  int op;
	
	printf("========================================\n\n");
	
	retOper = init_api();
	if(retOper == 1){
		printf("Falha na comunicacao com o cluster\n");
		exit(1);
	}
	
	op = 0;
	while(op != 9){
		printf("\n");
		printf("\nAPP Cliente ALOCS");
		printf("\n1 - create_server");
		printf("\n2 - create_dir");
		printf("\n3 - drop_dir");
		printf("\n4 - create_bucket");
		printf("\n5 - drop_bucket");
		printf("\n6 - put_pair");
		printf("\n7 - get_pair");
		printf("\n8 - remove_pair");
		printf("\n9 - fim");
		
		op = 0;
		printf("\nOperacao:");
		scanf("%d",&op);
    
		if(op == 1)
			app_create_server();
		else if(op == 2)
			app_create_dir();
		else if(op == 3)
			app_drop_dir();
		else if(op == 4)
			app_create_bucket();
		else if(op == 5)
			app_drop_bucket();
		else if(op == 6)
			app_put_pair();
		else if(op == 7)
			app_get_pair();
		else if(op == 8)
			app_remove_pair();
	};
	
  fin_api();
  
	printf("Executado com SUCESSO! \n");
	printf("\n========================================\n");
		
	return 0;
}
