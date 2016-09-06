#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include "util.h"

#ifndef CACHE_H
#define CACHE_H

#define AVALUE ((sqrt(5) - 1) / 2)
#define HASH(k) ( INDX_SIZE * fmod((k * AVALUE),1) )
#define BUFF_SLOT(i) ( (i * LIMIT_SIZE_BUCKET) + 0 )

//ajustar conforme o ambiente que esta sendo utilizado
#define BUFFER_LIMIT 500
#define INDX_SIZE 500

typedef struct ITEM_S{
    char *key;
    char *path;
    int hits;
    unsigned char *buffer;
    struct ITEM_S *next;
}ITEM_T;

typedef struct INDEX_S{
	ITEM_T *item;
}INDEX_T;

/*prototipo int init_cache()
 * descricao: inicializa o cache no ALOCS, esta funcao e chamada na funcao init no common.c
 * parametros:
 * retorno: um inteiro indicativo de falha ou sucesso*/
int init_cache();

/*prototipo int fin_cache()
 * descricao: libera a memoria utilizada pelo cache ao finalizar o ALOCS
 * parametros:
 * retorno: um inteiro indicativo de falha ou sucesso*/
int fin_cache();

/*prototipo: int put_buffer(char* key,char *path,unsigned char** buffer)
 * descricao: adiciona um bucket no cache retornando um ponteiro para o buffer designado
 * parametros: key ->   idBucket do Bucket que esta sendo adicionado ao cache
 * 						 path ->  path do bucket no metadados por enquanto nao esta sendo utilizado
 * 						 buffer-> espaco no cache designado para o bucket
 * retorno: um inteiro indicativo de falha ou sucesso na obtencao do cache*/
int put_buffer(char* key,char *path,unsigned char** buffer);

/*prototipo: int get_buffer(char* key, unsigned char* buffer)
 * descricao: localiza o cache referente ao bucket identificado por parametro
 * parametros: key -> id do Bucket que que sera pesquisado
 * 						 buffer -> aponta para o buffer do bucket no cache
 * retorno: um inteiro indicativo de falha ou sucesso na busca pelo bucket*/
int get_buffer(char* key, unsigned char** buffer);

/*prototipo: int remove_buffer(char* key)
 * descricao: remove do cache o bucket identificado pela chave passada por parametro
 * parametros: key ->   idBucket do Bucket que esta no cache
 * retorno: um inteiro indicativo de falha ou sucesso na obtencao do cache*/
int remove_buffer(char* key);

/*prototipo: unsigned int get_hash(char* str)
 * descricao: atribui um hash para a localizacao do bucket no cache
 * parametros: str ->   idBucket do Bucket que esta no cache
 * retorno: o valor de hash para o Bucket*/
unsigned int get_hash(char* str);

//tabela hash para localizacao dos buckets no cache
INDEX_T *INDEX;

//buffer para o cache
unsigned char *BUFFER;

#endif
