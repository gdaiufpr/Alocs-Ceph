/*
* Arquivo: alocs.c
* Data: 26/02/2014
* Numero de Serie: 20140226002
*
************************************************************************
* Nota: Switch resolvido. Ate o presente momento, o put e get estao funcionado redondinho.
* O tratamento em relacao a quantidade de argumentos passados por parametro
* tambem esta ok. todos estao aceitando a quantia adequada e alertando sobre o mau uso.
* get() esta ok.
* Todas as opcoes estao funcionado corretamente.
* O elementar solicitado pela profa Carmem esta [OK]
*
* Versao: 0.4
* Descricao: Este arquivo faz a camada entre a aplicacao e o
* sistema de arquivos distribuido. Alem disso, conversa com o metadados.
************************************************************************
* Versao 2.0
*
* Notas: Nesta versao, ainda nao existe a integracao com o Metadados, o que depende dele, ou e
* passado como parametro ou entao esta fixado.
** O alocs chama as funcoes do ceph por meio da biblioteca iceph, ela e de fato quem
* conversa com o ceph. Seus retornos sao de 0.
**
*TODO: funcoes a implementar
*      clean() / replicate_dir() / dropALL_dir
*
*/

#include "libalocs.h"
#include <stdio.h>
#include <stdlib.h>
#include <jni.h>
#include "imds.h"
#include <time.h>

int retOper;

//void aponta_dir(char *bucket,char *srvName,char *dirName);

/********************************************
 *inicio funcoes de gerenciamento
 ********************************************/
int init_api(){

	retOper = initialize();

	return retOper;
}

int fin_api(){

	retOper = finalize();

	return retOper;
}

/********************************************
 *inicio operacoes interface da aplicacao
 ********************************************/

/*assinatura: int clean()
 *descricao: requisita a remoção de todos os Buckets que estiverem vazios*/

int clean(void){
	return 0;
}

/*assinatura: int create_server(char *srvName)
 *descricao: Requisita a criação do Servidor especificado nos parametros de entrada*/
int create_server(char *srvName){
	//setar o env java
	JavaVM *jvm;
   	JNIEnv *env;
    	env = create_vm(&jvm);
	jstring path = (*env)->NewStringUTF(env, srvName);
	//chamda do ceph
	retOper = ss_create_server(srvName);

	clock_t start = clock();
	//chamada do metadados

	invoke_class_createServer(env, path);

	clock_t stop = clock();
	double elapsed = (long double)(stop - start)* 1000.0 / CLOCKS_PER_SEC;
	printf("Time elapsed in ms: %f", elapsed);
	return retOper;
}

/*assinatura: int drop_server(char *srvName)
 *descricao: Requisita a remocao do Servidor especificado nos parametros de entrada*/
int drop_server(char *srvName){

	retOper = ss_drop_server(srvName);
	//Chamada do sistema de metadados para crear o server
	//invoke_class_deleteServer(env, srvName);

	return retOper;
}

/*assinatura: int create_dir(char *dirName,char *srvName)
 *descricao: Requisita a criação de um Bucket em Diretorio especificado nos parametros de entrada,
 *em conjunto com o identificador do Bucket, e o intervalo de chaves que sera armazenado*/
int create_dir(char *dirName,char *srvName){

	JavaVM *jvm;
    	JNIEnv *env;
   	 env = create_vm(&jvm);
	char buff[12];
	char buffc[22];

	snprintf(buff, 11, "%s%s",srvName, "/");
    	snprintf(buffc, 22, "%s%s",buff,dirName);
	fprintf(stdout,"Directory path -> %s!\n",buffc);


	jstring path = (*env)->NewStringUTF(env, buffc);
	// fazer a chamada na interface SA para criacao do diretorio
	retOper = ss_create_dir(dirName,srvName);

	clock_t start = clock();

	// chamada de metadados para crear o diretorio
	invoke_class_createDir(env,path);

	clock_t stop = clock();
	double elapsed = (long double)(stop - start)* 1000.0 / CLOCKS_PER_SEC;
	printf("Time elapsed in ms: %f", elapsed);
	return retOper;
}

/*assinatura: int drop_dir(char *dirName,char *srvName)
 *descricao: Requisita a remocao de um Diretorio, especificado nos parametros de entrada,
 *em conjunto com o Servidor em que ele esta alocado*/
int drop_dir(char *dirName,char *srvName){

	/*sequencia de operacoes
	 *1)remover diretorio no SA
	 *2)atualizar metadados*/

	//d) drop dir
	retOper = ss_drop_dir(dirName,srvName);

	/*2) atualizar metadados
	if(retOper == 0)
		retOper = drop_dir(dirName,srvName);

	*/

	return retOper;
}

/*assinatura: int replicate_dir(char *dirName,char *srvName)
 *descricao: Requisita a replicacao de um Diretorio, especificado como parametro de entrada,
 *em conjunto com o Servidor de destino.*/
int replicate_dir(char *dirName,char *srvName){

	/*sequencia de operacoes
	 *1)solicitar ao metadados a localidade do servidor
	 *2)fazer a chamada na interface SA para copia do diretorio
	 *3)atualizar os metadados*/

	/*1) solicitar ao metadados a localidade do servidor
	locality = get(dirName);*/

	/*2) fazer a chamada na interface SA para criacao do diretorio
	retOper = copy_dir(dirName,locality->srvName,srvName);*/

	/*3) atualizar metadados
	retOper = put_dir(dirName,srvName);
	*/
	return 0;
}

/*assinatura: int drop_ALLdir(char *dirName)
 *descricao: Requisita a remocao de um Diretorio, e suas replicas, especificado como
 *parametro de entrada.*/
int dropALL_dir(char *dirName){
	return 0;
}

/*assinatura: int create_bucket(char *dirName,char *idBucket,double inikey,double finkey)
 *descricao: Requisita a criação de um Bucket em Diretorio especificado nos parametros de entrada,
 *em conjunto com o identificador do Bucket, e o intervalo de chaves que sera armazenado*/
int create_bucket(char *srvName,char *dirName,char *idBucket,uint64_t iniKey,uint64_t finKey){

	/* No futuro, verificar se o create_bucket avisa se
	* um bucket com mesmo nome ja existe, hoje,
	* simplesmente sobrescreve
	*/
	JavaVM *jvm;
    	JNIEnv *env;
    	env = create_vm(&jvm);

	int retOper = 1;
	unsigned int maxKeys;

	char path[32];

	strcpy(path,srvName);
	strcat(path,"/");
    	strcat(path,dirName);
	strcat(path,"/");
	strcat(path,idBucket);

	jstring pathj = (*env)->NewStringUTF(env, path);

	/*chamada da operacao create_bucket da interface S.A.
	 *srvName deve ser buscado no metadados*/
	maxKeys = (finKey - iniKey)+1;

	clock_t start = clock();
	retOper = ss_create_bucket(srvName,dirName,idBucket,maxKeys);

	/*atualizar metadados*/
	invoke_class_createBucket(env, pathj, iniKey, finKey);
	/*if(retOper == 1)
		retOper = put_bucket(idBucket,dirName,iniKey,finKey);
	*/
	clock_t stop = clock();
	double elapsed = (long double)(stop - start)* 1000.0 / CLOCKS_PER_SEC;
	printf("Time elapsed in ms: %f", elapsed);

	return retOper;
}

/*assinatura: int drop_bucket(char *idBucket)
 *descricao: Requisita a remocao de um Bucket especificado nos parametros de entrada.*/
int drop_bucket(char *idBucket){

	char dirName[10];
	char srvName[10];

	//temporario enquanto nao liga com mds
	//aponta_dir(idBucket,srvName,dirName);

	/*sequencia de operacoes
	 *1)obter localizacao do bucket
	 *2)remover bucket no SA
	 *3)atualizar metadados*/

	/*1) obter localizacao do bucket
	locality deve ser um struct para colocar na sequencia servidor/diretorio
	get(idBucket) deve retornar uma lista caso o diretorio seja replicado
	locality = get(idBucket);*/

	//2) remover bucket no SA
	retOper = ss_drop_bucket(idBucket, dirName,srvName);

	/*3) atualizar metadados
	if(retOper == 0)
		retOper = drop_bucket(idBucket,dirName);*/

	return retOper;
}

/*assinatura: int get_pair(KEY_T key)
 *descricao: Requisita um par chave-valor identificado por uma chave especificada como parametro de entrada.
 *O Bucket e identificado pelo sistema de Metadados baseado no intervalo de chaves*/
int get_pair(KEY_T key,PAIR_T *pair){

	int hit;
	BUCKET_T buff_bucket;
	hit = -1;

	JavaVM *jvm;
    	JNIEnv *env;
    	env = create_vm(&jvm);
	/*sequencia de operacoes
	 *1)obter localizacao da chave
	 *2)solicitar ao SA o bucket
	 *3)extrair o par chave_valor*/

	/*1) obter localizacao do bucket*/
	char *srvName;
	char *dirName;
	char *idBucket;

	clock_t start = clock();

	const char * locality = invoke_class_getKeyLocation(env, key);
	char buff[35];
	strcpy(buff, locality);
	srvName=buff;
	strtok_r(buff,"/",&dirName);
	strtok_r(dirName,"/",&idBucket);
	/*2) solicitar o bucket ao cache ou ao SA
		buff_bucket e atualizado com o buffer do bucket, hit indica se esta utilizando cache*/



	retOper = ss_get_bucket(srvName,dirName,idBucket,&buff_bucket,&hit);
	if(retOper == 0){ //get_bucket retorna 1 para falha e 0 para sucesso
		//3) extrair o par chave_valor
		retOper = extractPair(&buff_bucket,key,pair);

		//se nao tiver utilizando cache a area alocada deve ser liberada
		if(hit == 0)
			free(buff_bucket);
	}

	clock_t stop = clock();
	double elapsed = (long double)(stop - start)* 1000.0 / CLOCKS_PER_SEC;
	printf("Time elapsed in ms: %f", elapsed);

	return retOper;

}

/*assinatura: int put_pair(KEY_T key,char *value)
 *descricao: Requisita a adicao de um par chave-valor especificado nos parametro de entrada.
 *O Bucket e identificado pelo sistema de Metadados baseado no intervalo de chaves*/
int put_pair(KEY_T key,char *value){

	JavaVM *jvm;
   	JNIEnv *env;
    	env = create_vm(&jvm);

	//posteriormente estas variaveis talves sejam eliminadas
	char *srvName;
	char *dirName;
	char *idBucket;

	/*sequencia de operacoes
	 *1)obter localizacao da chave
	 *2)solicitar ao SA a adicao da chave
	 *3)retornar para a aplicacao*/

	clock_t start = clock();
	/*1) obter localizacao da chave*/
	const char * locality = invoke_class_getKeyLocation(env, key);
	//2) solicitar ao SA a adicao da chave
	char buff[12];
	strcpy(buff, locality);
	srvName=buff;
	strtok_r(buff,"/",&dirName);
	strtok_r(dirName,"/",&idBucket);

	printf("Server: '%s'\n Directory: '%s'\n Bucket: '%s'\n", srvName, dirName, idBucket);
	retOper = ss_put_pair(srvName, dirName, idBucket, key, value);

	clock_t stop = clock();
	double elapsed = (long double)(stop - start)* 1000.0 / CLOCKS_PER_SEC;
	printf("Time elapsed in ms: %f", elapsed);

	return retOper;
}

/*assinatura: int rem_pair(KEY_T key)
 *descricao: Requisita a remoção de um par chave-valor identificado por uma chave especificada como parametro
 *de entrada. O Bucket é identificado pelo sistema de Metadados baseado no intervalo de chaves*/
int rem_pair(KEY_T key){

	char srvName[10];
	char dirName[10];
	char idBucket[10];

	//eliminar posteriormente
	strcpy(idBucket,"BUCK1CT");
	//aponta_dir(idBucket,srvName,dirName);

	/*sequencia de operacoes
	 *1)obter localizacao da chave
	 *2)solicitar ao SA a remocao da chave
	 *3)retornar para a aplicacao*/

	/*1) obter localizacao da chave
	locality deve ser um struct para colocar na sequencia servidor/diretorio
	get(k) deve retornar uma lista caso o diretorio seja replicado
	locality = get(key);*/

	//2) solicitar ao SA a remocao do par
	retOper = ss_remove_pair(srvName, dirName, idBucket, key);

	return retOper;
}
/*
void aponta_dir(char *bucket,char *srvName,char *dirName){

	strcpy(srvName,"PARANA");

	//artificio para buscar o diretorio enquanto nao coloca metadados
	if(strcmp(strstr(bucket,"CT"),"CT") == 0 )
	  strcpy(dirName,"CURITIBA");
	else if	(strcmp(strstr(bucket,"CV"),"CV") == 0 )
		strcpy(dirName,"CASCAVEL");
  	else if	(strcmp(strstr(bucket,"CL"),"CL") == 0 )
		strcpy(dirName,"COLORADO");
	else if	(strcmp(strstr(bucket,"TD"),"TD") == 0 )
		strcpy(dirName,"TOLEDO");
	else if	(strcmp(strstr(bucket,"MD"),"MD") == 0 )
		strcpy(dirName,"MEDIANEIRA");
}*/
