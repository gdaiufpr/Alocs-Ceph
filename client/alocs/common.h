#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

#ifndef COMMON_H
#define COMMON_H

/*
* constantes para manipulacao do bucket
*/
#define BASE 95
#define BYTES_LIMIT 4
#define SLOT_SIZE 18
#define OFF_SLOT_SIZE 4
#define LEN_SLOT_SIZE 4
#define KEY_SLOT_SIZE 10

//definicao do limite de tamanho do bucket
#define LIMIT_SIZE_BUCKET 64000

//posicao do offset no slot
#define OFF_POS_SLOT(s) (s)

/*tamanho do header é definido pela qtd de chaves vezes o tamanho do slot + o tamanho do mapa
 *de bits que e equivalente a qtd de chaves + 1(null)*/
#define HEADER_SIZE(k) ((k * SLOT_SIZE) + (k + 1))
//obtem a posicao do slot livre obtido a partir do bitmap
#define GET_SLOT_POS(p,k) ( ((p * SLOT_SIZE) + k) )
//posicao do offset no slot
#define OFF_POS_SLOT(s) (s)
//posicao do length no slot
#define LEN_POS_SLOT(s) (s+4)
//posicao de key no slot
#define KEY_POS_SLOT(s) (s+8)

//definicao do tipo das variaveis que representarem a chave
//TODO: pensar no tipo de dado da chave, o slot é um char[10]s
//typedef uint64_t KEY_T;
typedef unsigned char *KEY_T;

typedef unsigned char *BUCKET_T;

typedef unsigned char *PAIR_T;

typedef struct S_LOCALITY{
		char *srvName;
		char *dirName;
		char *idBucket;
}T_LOCALITY;

int extractPair(BUCKET_T *buff_bucket,KEY_T key,PAIR_T *p_pair);
int find_slot_free(unsigned char **p_header,int numKeys,int len_value,int *offset);
int find_slot_key(unsigned char **p_header,int numKeys,KEY_T key,int pos,KEY_T slot_key);
void int_to_str(unsigned char *str, int number);
int str_to_int(unsigned char *str);
void copy_buffer_key(unsigned char *dest, unsigned char *src);

#endif
