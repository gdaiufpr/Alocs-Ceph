#include "util.h"

/**********************************
DEFINICAO FUNCOES UTEIS.
**********************************/

/*prototipo void itoc(unsigned char *str,size_t length, int number)
 * descricao: converte um int para char
 * parametros: str ->ponteiro para a string com o retorno
 * 						 length -> sizeof de str
 *             number -> inteiro a ser convertido
 * retorno: string contendo o inteiro*/
void itoc(unsigned char *str,size_t length, int number){

  snprintf(str,length,"%"PRIu32,number);
}

/*prototipo void itoc64(unsigned char *str,size_t length, uint64_t number)
 * descricao: converte um uint64_t para char
 * parametros: str ->ponteiro para a string com o retorno
 * 						 length -> sizeof de str
 *             number -> inteiro a ser convertido
 * retorno: string contendo o uint64_t*/
void itoc64(unsigned char *str,size_t length, uint64_t number){

  snprintf(str,length,"%"PRIu64,number);
}

uint64_t ctoi(char *str,int int32){

    uint64_t ret;

    ret = ((int32) ? atoi(str) : atol(str));

    return ret;
}

/*prototipo void ntochr(char* str, int number)
 * descricao: converte um inteiro para um caracter de 4 bytes,funcao utilizada
 * 						para guardar o length e o offset no slot limitando o tamanho a 4 bytes
 * parametros: str ->ponteiro para o caracter
 *             number ->inteiro  que ser convertido
 * retorno: str com o char de 4 bytes*/
void ntochr(char* str, int number){
	int i, pos,inc;

      inc = 32;

	memset(str,'\0',BYTES_LIMIT+1);

	pos = BYTES_LIMIT - 1;
	while(number >= BASE) {
		str[pos--] = (number % BASE);
		number /= BASE;
	}

	str[pos] = number;

	for(i = 0; i < BYTES_LIMIT; i++)
  		str[i] = inc + str[i];
}

/*prototipo int chrton(unsigned char* str)
 * descricao: converte um um caracter de 4 bytes para um inteiro, funcao utilizada para
 * 						guardar o length e o offset no slot limitando o tamanho a 4 bytes
 * parametros: str -> ponteiro para o caracter
 * retorno: um inteiro com o numero convertido*/
int chrton(char* str){
	unsigned int number;
       int pos,e,inc;

	inc = 32;

	for(pos = BYTES_LIMIT-1,e = 0;pos >=0;pos--,e++)
		number += (str[pos] - inc) * get_exp(BASE,e);

	return number;
}

/*prototipo exp(int,int)
 * descricao: calculo da exponenciacao, para a conversao de char para inteiro,
 * 						utilizada na funcao chrton
 * parametros: x -> base
 * 						 n -> expoente
 * retorno: inteiro com o resultado do calculo*/
int get_exp(int x, int n){

    if(n == 0)
       return 1;

    return x * get_exp(x, n - 1);
}

/*prototipo void *xmalloc(size_t size)
 * descricao: aloca memoria de um determinado tamanho e retorna o ponteiro
 * parametros: size -> tamanho da memoria a ser alocado
 * retorno: ponteiro para a memoria alocada*/
void *xmalloc(size_t size){

	void *pointer = malloc(size);
       if(pointer == NULL){
          fprintf(stderr,"Erro ao alocar memória!\n");
	   exit(EXIT_FAILURE);
  }

	return pointer;
}

