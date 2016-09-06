#include "alocs.h"

#ifndef LIBALOCS_H
#define LIBALOCS_H

/***************************************
 DEFINICAO FUNCOES DE GERENCIAMENTNO
***************************************/
extern int init_ssystem(void);

extern int fin_ssystem(void);

extern int init_api(void);

extern int fin_api(void);


/****************************
	DEFINICOES OPERACOES INTERFACE DE APLICACAO
 ****************************/

/*assinatura: int clean()
 *descricao: requisita a remoção de todos os Buckets que estiverem vazios*/
extern int clean();

/*assinatura: int create_bucket(char *dirName,char *idBucket,double inikey,double finkey)
 *descricao: Requisita a criação de um Bucket em Diretorio especificado nos parametros de entrada,
 *em conjunto com o identificador do Bucket, e o intervalo de chaves que sera armazenado*/
extern int create_bucket(char *dirName,char *idBucket,KEY_T inikey,KEY_T finkey);

/*assinatura: int drop_bucket(char *idBucket)
 *descricao: Requisita a remocao de um Bucket especificado nos parametros de entrada.*/
extern int drop_bucket(char *idBucket);

/*assinatura: int create_dir(char *dirName,char *srvName)
 *descricao: Requisita a criação de um Bucket em Diretorio especificado nos parametros de entrada,
 *em conjunto com o identificador do Bucket, e o intervalo de chaves que sera armazenado*/
extern int create_dir(char *dirName,char *srvName);

/*assinatura: int drop_dir(char *dirName,char *srvName)
 *descricao: Requisita a remocao de um Diretorio, especificado nos parametros de entrada,
 *em conjunto com o Servidor em que ele esta alocado*/
extern int drop_dir(char *dirName,char *srvName);

/*assinatura: int drop_ALLdir(char *dirName)
 *descricao: Requisita a remocao de um Diretorio, e suas replicas, especificado como
 *parametro de entrada.*/
extern int drop_ALLdir(char *dirName);

/*assinatura: int replicate_dir(char *dirName,char *srvName)
 *descricao: Requisita a replicacao de um Diretorio, especificado como parametro de entrada,
 *em conjunto com o Servidor de destino.*/
extern int replicate_dir(char *dirName,char *srvName);

/*assinatura: int get_pair(KEY_T key)
 *descricao: Requisita um par chave-valor identificado por uma chave especificada como parametro de entrada.
 *O Bucket e identificado pelo sistema de Metadados baseado no intervalo de chaves*/
extern int get_pair(KEY_T key,PAIR_T *pair);

/*assinatura: int put_pair(KEY_T key,char *value)
 *descricao: Requisita a adicao de um par chave-valor especificado nos parametro de entrada.
 *O Bucket e identificado pelo sistema de Metadados baseado no intervalo de chaves*/
extern int put_pair(KEY_T key,char *value);

/*assinatura: int rem_pair(KEY_T key)
 *descricao: Requisita a remoção de um par chave-valor identificado por uma chave especificada como parametro
 *de entrada. O Bucket é identificado pelo sistema de Metadados baseado no intervalo de chaves*/
extern int rem_pair(KEY_T key);

#endif
