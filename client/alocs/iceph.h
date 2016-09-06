#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include "common.h"
#include "ceph_lib.h"

#ifndef ICEPH_H
#define ICEPH_H

//variavel que recebe o retorno das operacoes 
unsigned int state;

/*
* Variaveis usadas por todas as funcoes
* Usadas para comunicacao com o ceph
*/
rados_t cluster;  // variavel tipo rados_t que manipula o cluster
rados_ioctx_t ioctx;  //variavel de contexto de io
rados_read_op_t read_op; //variavel que armazena o handle para operacoes de escrita

/**********************************
DEFINICAO OPERACOES INTERFACE S.A.
**********************************/

/*assinatura: int create_bucket(char *srvName,char *dirName,char *idBucket)
 *objetivo: Cria um Bucket em Diretorio especificado nos parametros de entrada, em conjunto com o
 *					 identificador do Bucket, e Servidor.*/
int ss_create_bucket(char *srvName,char *dirName,char *idBucket,int maxKeys);

/*assinatura: int drop_bucket(char *idBucket,char *dirName,char *srvrName)
 *objetivo: Remove um Bucket, do Servidor e Diretório especificados por
 *parametros em conjunto com o identificador do Bucket*/
int ss_drop_bucket(char *idBucket,char *dirName,char *srvName);

/*assinatura: int getBucket(char *srvName,char *dirName,char *idBucket)
 *objetivo: Retorna um Bucket, de um Diretorio e Servidor especificados nos parametros de entrada*/
int ss_get_bucket(char *srvName,char *dirName,char *idBucket, BUCKET_T *buff_bucket);

/*assinatura: int create_bucket(char *dirName,char *srvName)
 *objetivo: Cria um Diretorio em Servidor especificado por parametro.*/
int ss_create_dir(char *dirName,char *srvName);

/*assinatura: int copy_dir(char *dirName,char *srvName1,char *srvName2)
 *objetivo: Copia um Diretorio de um Servidor para outro Servidor, ambos especificados por parametros.*/
int ss_copy_dir(char *dirName,char *srvName1,char *srvName2);

/*assinatura: int drop_dir(char *dirName,char *srvName)
 *objetivo: Remove um Diretorio de um Servidor, ambos especificados por parametros.*/
int ss_drop_dir(char *dirName,char *srvName);

/*assinatura: int is_Empty(char *idBucket,char *dirName,char *srvName)
 *objetivo: Verifica se um Bucket, especificado por parametro, esta vazio, são indicados tambem o Diretorio e Servidor*/
int ss_is_empty_bucket(char *idBucket,char *dirName,char *srvName);

/*assinatura: int is_Empty(char *dirName,char *srvName)
 *objetivo: Verifica se um Diretório, especificado por parametro, está vazio, deve ser indicado tambem o Servidor*/
int ss_is_empty_dir(char *dirName,char *srvName);

/*assinatura: int put_pair(char *srvName,char *dirName,char *idBucket,KEY_T key,char *value)
 *objetivo: Verifica se um Diretório, especificado por parametro, está vazio, deve ser indicado tambem o Servidor*/
int ss_put_pair(char *srvName,char *dirName,char *idBucket,KEY_T key,char *value);

/*assinatura: int (char *srvName,char *dirName,char *idBucket,KEY_T key)
 *objetivo: Remove um par chave-valor do Bucket correspondente ao identificador passado como parametro
 *de entrada, e necessario fornecer tambem o Servidor e Diretorio*/
int ss_remove_pair(char *srvName,char *dirName,char *idBucket,KEY_T key);

/*
***********************************
FUNCOES DE GERENCIAMENTO
***********************************
*/

int init_rados(void);

int fin_rados(void);

#endif
