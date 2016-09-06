#include "common.h"

int ret;

/*prototipo: int initialize()
 * descricao: inicializa os componentes da API, sistema de armazenamento,metadados e cache
 * parametros: */
int initialize(){

  //inicializa sistema de armazenamento
	ret = init_ssystem();
	if(ret == 0) //inicializa o cache se o sistema de armazenamento iniciar
    	ret = init_cache();

	return ret;
}

/*prototipo: int finalize()
 * descricao: finaliza os componentes da API, sistema de armazenamento,metadados e cache
 * parametros: */
int finalize(){

	//finaliza o sistema de armazemento
	ret = fin_ssystem();
	//finaliza o cache
	ret = fin_cache();

	return ret;
}

int init_ssystem(){
	ret = init_rados();

	return ret;
}

int fin_ssystem(){
	ret = fin_rados();

	return ret;
}

/*prototipo void set_offset_slot(unsigned char**,int)
 * descricao: grava o valor do offset no slot
 * parametros: src ->ponteiro para a posicao do offset no slot
 * 						 offset ->valor que sera gravado*/
void set_offset_slot(unsigned char **src,unsigned int offset){

  char off_c[OFF_SLOT_SIZE+1];

	ntochr(off_c,offset);

	strncpy(OFF_POS_SLOT(*src),off_c,OFF_SLOT_SIZE);

}

/*prototipo int get_offset_slot(unsigned char*)
 * descricao: recupera o valor do offset no slot
 * parametros: src ->ponteiro para a posicao do offset no slot
 * retorno: um inteiro correspondente ao offset*/
unsigned int get_offset_slot(unsigned char *src){

	int offset;
	char off_c[OFF_SLOT_SIZE];

	offset = 0;

	strncpy(off_c,OFF_POS_SLOT(src),OFF_SLOT_SIZE);

	if(strlen(off_c) > 0)
		offset = chrton(off_c);

	return offset;
}

/*prototipo void set_len_slot(unsigned char**,int)
 * descricao: grava o valor do length no slot
 * parametros: src ->ponteiro para a posicao do length no slot
 * 						 length ->valor que sera gravado*/
void set_len_slot(unsigned char **src,unsigned int length){

  char len_c[LEN_SLOT_SIZE+1];

	ntochr(len_c,length);

	strncpy(LEN_POS_SLOT(*src),len_c,LEN_SLOT_SIZE);
}

/*prototipo int get_len_slot(unsigned char*)
 * descricao: recupera o valor do length no slot
 * parametros: src ->ponteiro para a posicao do length no slot
 * retorno: um inteiro correspondente ao length*/
unsigned int get_len_slot(unsigned char *src){

  int len;
	char len_c[LEN_SLOT_SIZE];

	len = 0;

	strncpy(len_c,LEN_POS_SLOT(src),LEN_SLOT_SIZE);

	if(strlen(len_c) > 0)
		len = chrton(len_c);

	return len;
}

/*prototipo void set_key_slot(unsigned char**,KEY_T)
 * descricao: grava o valor da chave no slot
 * parametros: src ->ponteiro para a posicao da chave no slot
 * 						 key ->chave que sera gravado*/
void set_key_slot(unsigned char **src,KEY_T key){

  unsigned char key_c[KEY_SLOT_SIZE];

	itoc64(key_c,sizeof(key_c),key);

	strncpy(KEY_POS_SLOT(*src),key_c,KEY_SLOT_SIZE);
}

/*prototipo KEY_T get_key_slot(unsigned char*)
 * descricao: recupera o valor da chave no slot
 * parametros: src ->ponteiro para a posicao da chave no slot
 * retorno: um inteiro correspondente a chave*/
KEY_T get_key_slot(unsigned char *src){

  KEY_T key;
	unsigned char key_c[KEY_SLOT_SIZE];

	strncpy(key_c,KEY_POS_SLOT(src),KEY_SLOT_SIZE);
	key = ctoi(key_c,0);

	return key;
}

/*prototipo void set_slot(unsigned char**,unsigned int,unsigned int,KEY_T)
 * descricao: atualiza o slot no header
 * parametros: src ->ponteiro para a posicao inicial do slot
 * 						 offset -> valor do offset que sera gravado
 * 						 length -> valor do length que sera gravado
 * 						 key -> valor da chave que sera gravada
 * retorno: ponteiro para o slot atualizado*/
void set_slot(unsigned char **p_header,unsigned int slot_pos, unsigned int offset,unsigned int length,KEY_T key){

  unsigned char *p_slot,*slot;

	//aloca memoria para o slot
	slot = (unsigned char*) xmalloc(sizeof(unsigned char) * SLOT_SIZE);

	//insere no slot o offset para o valor
	set_offset_slot(&slot,offset);
	//insere no slot o tamanho do valor
	set_len_slot(&slot,length);
	//insere no slot a chave que identifica o par
	set_key_slot(&slot,key);

	p_slot = *p_header+slot_pos;
	strncpy(p_slot,slot,SLOT_SIZE);

	free(slot);
}


/*prototipo int find_slot_key(unsigned char**,int,KEY_T)
 * descricao: busca a chave passada por parametro no slots
 * parametros: p_buffer ->ponteiro para a posicao inicial
 * 												do mapa de bits no header
 *             numKeys -> qtd max de chaves no bucket, para limitar a busca no mapa de bits
 * 						 key -> chave requisitada
 * retorno: a posicao do slot no mapa de bits*/
int find_slot_key(unsigned char **p_buffer,int numKeys,KEY_T key){

	unsigned char *header;
	int slot_pos,pos,find;
	char bit;
	KEY_T slot_key;

	bit = '1';
	//posiciona o ponteiro do header, na posicao inicial do mapa de bits
	header = *p_buffer;

	/*pos indica a posicao no mapa de bits que sera retornado
	 *comeca em offset -1 pois na iteracao pos e incrementado
	  antes de utilizar o valor para que seja retornada a posicao exata*/
	pos = -1;
	slot_pos = numKeys;
	find = 0;

	//(pos < numKeys) evita que o loop extrapole o mapa de bits
	while((!find) && (pos < numKeys)){
		bit = header[++pos];

		if(bit == '1'){
			slot_pos =  GET_SLOT_POS(pos,numKeys);
			slot_key = get_key_slot(header+slot_pos);

			find = (key == slot_key);
		}
	}

	return ((find == 1) ? pos : -1);
}

/*prototipo int extractPair(BUCKET_T *buff_bucket,KEY_T key,PAIR_T *p_pair)
 * descricao: busca a chave passada por parametro no slots
 * parametros: p_buffer ->ponteiro para a posicao inicial
 * 												do mapa de bits no header
 *             numKeys -> qtd max de chaves no bucket, para limitar a busca no mapa de bits
 * 						 key -> chave requisitada
 * retorno: a posicao do slot no mapa de bits*/
int extractPair(BUCKET_T *buff_bucket,KEY_T key,PAIR_T *p_pair){

	int slot_pos,bit_pos,numKeys;
	unsigned char numKeysChr[BYTES_LIMIT+1];
	unsigned char *value,*p_bitmap,*p_body;
	unsigned char key_c[KEY_SLOT_SIZE];
              unsigned int slot_length,slot_off,hsize;
	size_t value_size,pair_size;

	slot_pos = 0;
	bit_pos = -1;

	/*busca a qtd. max de chaves no bucket para
	delimitar as leituras no mapa de bits*/
	memcpy(numKeysChr,*buff_bucket,BYTES_LIMIT);

	numKeys = chrton(numKeysChr);

	hsize = HEADER_SIZE(numKeys);

	/*percorre o mapa de bits para localizar a chave
	 *se encontrar a chave, bit_pos e atualizado com a posicao do bit no mapa
	 *header + BYTES_LIMIT indica para desconsiderar os BYTES_LIMIT iniciais do header*/
	//p_bitmap aponta para a posicao inicial do bitmap
	p_bitmap = *buff_bucket+BYTES_LIMIT;
	bit_pos = find_slot_key(&p_bitmap,numKeys,key);

	if(bit_pos >= 0){
		//obtem posicao do slot a partir da posicao no mapa
		slot_pos = GET_SLOT_POS(bit_pos,numKeys);

		slot_off = get_offset_slot(p_bitmap+slot_pos);
		slot_length = get_len_slot(p_bitmap+slot_pos);

		//converte o valor da chave e inteiro para char
		itoc64(key_c,sizeof(key_c),key);

		/*aloca memoria para o par, que e igual ao tamanho de key + separador +
		 *o tamanho do slot + o caracter nulo*/
		pair_size = KEY_SLOT_SIZE + slot_length + 2;
		*p_pair = (PAIR_T) xmalloc(pair_size);
		//inicializa a memoria alocada
		memset(*p_pair,'\0',pair_size);

		//aponta para a posicao do valor do slot
		p_body = *buff_bucket+slot_off;

    //aloca memoria para escrever o valor
		value_size = (sizeof(unsigned char) * slot_length) + 1;
		value = (unsigned char *) xmalloc(value_size);
		//inicializa a memoria alocada
		memset(value,'\0',value_size);

		//transfere o valor do bucket para value
		memcpy(value,p_body,slot_length);

		//monta o par chave-valor
		snprintf(*p_pair,pair_size,"%s\n%s",key_c,value);

		free(value);

		return 0;
	}else
		return 1;
}

/*prototipo int find_slot_free(unsigned char**,int,int,int*)
 * descricao: busca um slot livre no bucket
 * parametros: p_buffer ->ponteiro para a posicao inicial
 * 												do mapa de bits no header
 *             numKeys -> qtd max de chaves no bucket, para limitar a busca no mapa de bits
 * 						 len_value -> length no value necessário para verificar
 * 													se o tamanho do valor e compativel com o slot
 * 						 offset -> retorna o offset para insercao do valor
 * retorno: a posicao do slot no mapa de bits*/
int find_slot_free(unsigned char **p_buffer,int numKeys,int len_value,int *offset){

	unsigned char *p_header;
	int pos,slot_pos,find;
	unsigned int slot_length,prior_offset,prior_length;
	char bit;

	slot_length = find = prior_length = 0;

	//p_header aponta para o endereco de memoria de header
	p_header = *p_buffer;

	bit = '1';
	pos = -1;
	slot_pos = numKeys; //o marcador do slot comeca um posicao apos o fim do mapa de bits
	prior_offset = *offset;
	while((!find) && (pos < numKeys)){
   	//usa o valor de pos, depois incrementa
		bit = p_header[++pos];
		slot_pos = GET_SLOT_POS(pos,numKeys);

		/*verifica o bit: se igual alimenta offset
		 *se igual 0 obtem a posicao do slot e verifica e a condicao de parada foi aceita*/
		if(bit == '0'){
		  /*considera a possibilidade do slot livre ja ter sido ocupado uma vez
			 * se slot_length for maior que 0, se verdadeiro, verifica se o length
			 * e compativel com o length do value*/
			slot_length = get_len_slot(p_header+slot_pos);
			if((slot_length == 0) || ((slot_length > 0) && (len_value <= slot_length)) ){
				//calcula o offset do slot livre
				*offset = prior_offset+prior_length;
				find = 1;
			}
		}else{
			//guarda o offset do slot ocupado
			prior_offset = get_offset_slot(p_header+slot_pos);
			//guarda o length do slot ocupado
			prior_length = get_len_slot(p_header+slot_pos);
		}
  }

	return pos;
}
