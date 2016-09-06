#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>

#ifndef UTIL_H
#define UTIL_H

#define BASE 95
#define BYTES_LIMIT 4

//definicao do limite de tamanho do bucket
#define LIMIT_SIZE_BUCKET 64000


/**********************************
DEFINICAO FUNCOES UTEIS.
**********************************/

/*prototipo void itoc(unsigned char *str,size_t length, int number)
 * descricao: converte um int para char
 * parametros: str ->ponteiro para a string com o retorno
 * 						 length -> sizeof de str
 *             number -> inteiro a ser convertido
 * retorno: string contendo o inteiro*/
void itoc(unsigned char *str,size_t length, int number);

/*prototipo void itoc64(unsigned char *str,size_t length, uint64_t number)
 * descricao: converte um uint64_t para char
 * parametros: str ->ponteiro para a string com o retorno
 * 						 length -> sizeof de str
 *             number -> inteiro a ser convertido
 * retorno: string contendo o uint64_t*/
void itoc64(unsigned char *str,size_t length, uint64_t number);

/*prototipo void ctoi(unsigned char*,int)
 * descricao: converte um char para int ou u_int64
 * parametros: str ->ponteiro para a char com o numero
 *             int32 -> indica se o inteiro e 32 ou 64, se for 64 o valor e 0
 * retorno: int  contendo o inteiro*/
uint64_t ctoi(char *str,int int32);

/*prototipo void ntochr(char* str, int number)
 * descricao: converte um inteiro para um caracter de 4 bytes,funcao utilizada
 * 						para guardar o length e o offset no slot limitando o tamanho a 4 bytes
 * parametros: str ->ponteiro para o caracter
 *             number ->inteiro  que ser convertido
 * retorno: str com o char de 4 bytes*/
void ntochr(char* str, int number);

/*prototipo int s4toi(unsigned char* str)
 * descricao: converte um um caracter de 4 bytes para um inteiro, funcao utilizada para
 * 						guardar o length e o offset no slot limitando o tamanho a 4 bytes
 * parametros: str -> ponteiro para o caracter
 * retorno: um inteiro com o numero convertido*/
int chrton(char* str);

/*prototipo exp(int,int)
 * descricao: calculo da exponenciacao, para a conversao de char para inteiro,
 * 						utilizada na funcao s4toi
 * parametros: x -> base
 * 						 n -> expoente
 * retorno: inteiro com o resultado do calculo*/
int get_exp(int x, int n);

/*prototipo void *xmalloc(size_t size)
 * descricao: aloca memoria de um determinado tamanho e retorna o ponteiro
 * parametros: size -> tamanho da memoria a ser alocado
 * retorno: ponteiro para a memoria alocada*/
void *xmalloc(size_t size);

#endif