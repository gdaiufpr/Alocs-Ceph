#include "cache.h"

unsigned int BUFF_SIZE;

/*prototipo int init_cache()
 * descricao: inicializa o cache no ALOCS, esta funcao e chamada na funcao init no common.c
 * parametros:
 * retorno: um inteiro indicativo de falha ou sucesso*/
int init_cache(){

	ITEM_T *item;
	INDEX_T *p_index;
              size_t len_buff;

	BUFF_SIZE = 0;

	//inicializa o indice
	INDEX = (INDEX_T *) xmalloc(sizeof(INDEX_T) * INDX_SIZE);

  p_index = INDEX+0;

  //inicializacao de item
  while((p_index != NULL) && (p_index < INDEX+INDX_SIZE)){
    p_index->item = NULL;
    p_index++;
  }

	//inicializa o buffer
	len_buff = sizeof(unsigned char *) * (BUFFER_LIMIT * LIMIT_SIZE_BUCKET);
	BUFFER = (unsigned char *) xmalloc(len_buff);

	fprintf(stdout,"cache inicilizado!\n");
}

/*prototipo int init_cache()
 * descricao: inicializa o cache no ALOCS, esta funcao e chamada na funcao init no common.c
 * parametros:
 * retorno: um inteiro indicativo de falha ou sucesso*/
int fin_cache(){

	ITEM_T *item;
	INDEX_T *p_index;

	item = NULL;
  p_index = NULL;

  p_index = INDEX+0;

  if(p_index){
    while((p_index != NULL) && (p_index < INDEX+INDX_SIZE)){
      item = p_index->item;
		  if(item){
        free(item->key);
		    free(item->path);
		    free(item);
		  }
		  p_index++;
	  }
	}

  free(INDEX);
	free(BUFFER);

	fprintf(stdout,"Finalizou API\n");
}

/*prototipo int put_buffer(char* key,char *path,unsigned char** buffer)
 * descricao: adiciona um bucket no cache retornando um ponteiro para o buffer designado
 * parametros: key ->   idBucket do Bucket que esta sendo adicionado ao cache
 * 						 path ->  path do bucket no metadados por enquanto nao esta sendo utilizado
 * 						 buffer-> espaco no cache designado para o bucket
 * retorno: um inteiro indicativo de falha ou sucesso na obtencao do cache*/
int put_buffer(char* key,char *path,unsigned char** buffer){

	ITEM_T *new_item,*item;
  INDEX_T *p_index;
	size_t len_path,len_key;

	p_index = NULL;
  new_item = NULL;

	p_index = INDEX+get_hash(key);

  //gravacao do indice
	if(p_index == NULL)
    return 0;

  if(p_index->item != NULL)
    item = p_index->item;
  else
    item = NULL;

	//aloca memoria para o novo item
	new_item = (ITEM_T *) xmalloc(sizeof(ITEM_T));

  //aloca memoria para o key, e uma string sem o tamanho previamente definido
  len_path = sizeof(char) * (strlen(path) + 1);
  new_item->path = (char *) xmalloc(len_path);

  //aloca memoria para o path, e uma string sem o tamanho previamente definido
  len_key = sizeof(char) * (strlen(key) + 1);
	new_item->key = (char *) xmalloc(len_key);

  //inicializa as areas de memoria
  memset(new_item->key,'\0',len_key);
  memset(new_item->path,'\0',len_path);

  strncpy(new_item->key,key,len_key);
	strncpy(new_item->path,path,len_path);

	new_item->hits = 1;

	if(BUFF_SIZE < BUFFER_LIMIT){
		*buffer = BUFFER+BUFF_SLOT(BUFF_SIZE);

		//buffer -> retorna para iceph.c a area disponivel no cache
		if(*buffer != NULL)
			new_item->buffer = *buffer;
	  else
			return 0;

		//incrementa o controle do buffer
		BUFF_SIZE++;
	}else //TODO:acertar renovacao do buffer
		return 0; //se nao tiver espaco no cache retorna 1 e feita a recuperacao a partir do disco

	new_item->next = NULL;

	//verifica se a posicao na tabela ja esta ocupada, se nao tiver inicia uma lista
	if(!item)
		p_index->item = new_item;
	else{
		//se ja houver uma lista percorre ate encontrar uma posicao vaga
		while(item->next != NULL)
			item = item->next;

		//adiciona no final da lista
		item->next = new_item;
	}

	return 1;
}

/*prototipo: int get_buffer(char* key, unsigned char* buffer)
 * descricao: localiza o cache referente ao bucket identificado por parametro
 * parametros: key -> id do Bucket que que sera pesquisado
 * 						 buffer -> aponta para o buffer do bucket no cache
 * retorno: um inteiro indicativo de falha ou sucesso na busca pelo bucket*/
int get_buffer(char* key, unsigned char** buffer){

	ITEM_T *item,*target;
	INDEX_T *p_index;

  p_index = NULL;
  target = item = NULL;

	p_index = INDEX+get_hash(key);

	//se a posicao encontrar nao tiver item retorna NULL
  if(!p_index->item)
    return 0;

  //se a chave do primeiro item for a procurada
	if(strcmp(p_index->item->key,key) == 0)
		target = p_index->item;
	else if(p_index->item->next){
    //se nao for o primeiro percorre a lista ate encontrar a chave
    item = p_index->item;
    while((item != NULL)&&(strcmp(item->key,key) != 0))
			item = item->next;

		target = item;
	}

	if(target != NULL){
		*buffer = target->buffer;

		return 1;
	}else
	  return 0;

}

/*prototipo: int remove_buffer(char* key)
 * descricao: remove do cache o bucket identificado pela chave passada por parametro
 * parametros: key ->   idBucket do Bucket que esta no cache
 * retorno: um inteiro indicativo de falha ou sucesso na obtencao do cache*/
int remove_buffer(char *key){

	ITEM_T *item;
	INDEX_T *p_index,*target;

  target = NULL;
  item = NULL;

	p_index = INDEX+get_hash(key);

  if(!p_index)
    return 0;

	if(p_index->item){
    target = p_index;
    //se a lista escandeada tiver mais de um item o primeiro nao e o desejado
    if((target->item->next) && (strcmp(target->item->key,key) != 0)){
      item = target->item;
      while((item != NULL)&&(strcmp(item->next->key,key) != 0 ))
				item = item->next;

			target->item = item->next;
			item->next = item->next->next;
    }
  }

  if(!target)
    return 1;

  memset(target->item->key,'\0',strlen(target->item->key));
  memset(target->item->path,'\0',strlen(target->item->path));
  memset(target->item->buffer,'\0',LIMIT_SIZE_BUCKET);

  free(target->item->key);
  target->item->key = NULL;

  free(target->item->path);
	target->item->path = NULL;

	target->item->buffer = NULL;

  free(target->item);
  target->item = NULL;

  p_index = NULL;
  target = NULL;

	return 0;

}

/*prototipo: unsigned int get_hash(char* str)
 * descricao: atribui um hash para a localizacao do bucket no cache
 * parametros: str ->   idBucket do Bucket que esta no cache*/
unsigned int get_hash(char* str){

  int i;
  unsigned int number,h;

  number = h = 0;

  for(i = 0;i < strlen(str);i++)
     number += *(str+i);

  h = HASH(number);

  return h;
}
