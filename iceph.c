/*
* Arquivo: iceph.c
* Data: 26/02/2014
* Versao: 1.0
* Descricao: Este arquivo e a camada que conversa com o ceph
*
*
* to_do: a funcao create_bucket ainda nao esta aceitando a criacao de bucket com um crush_rule
* por enquanto usa a default, o importante eh que funciona. (MUDAR PARA DIRETORIO)
*
* TODO:
* 	implementar as operacoes que faltam (is_Empty(),copy_dir())
*/
#include "iceph.h"

/*
***********************************************
* Funcoes de gerenciamento
***********************************************
*/

/*
* Funcao que inicia o rados(), conecta no cluster
* arquivo de configuracao: /etc/ceph/ceph.conf
*/
int init_rados() {

	// cria o manipulador do cluster
	create_handle(&cluster);
	if(cluster == NULL){
		fprintf(stderr,"[init_rados/iceph.c] cannot create a cluster handle\n");

		return 1;
	}

	return 0;
}

/*
* Funcao que termina a conexao com o rados
*
*/
int fin_rados(){

	cluster_shutdown(&cluster);  // desconecta do cluster

	return 0;
}

/**********************************
DEFINICAO OPERACOES INTERFACE S.A.
**********************************/

/*prototipo: int create_server(char *srvName)
 *objetivo: Cria o Servidor com o identificador especificado por parametro.*/
int ss_create_server(char *srvName){

	//definir identificador da regra
	uint8_t crush_ruleset = 0;

	state = create_pool(cluster,srvName,crush_ruleset);

	return ((state >= 0) ? 0 : 1);
}

/*prototipo: int create_server(char *srvName)
 *objetivo: Cria o Servidor com o identificador especificado por parametro.*/
int ss_drop_server(char *srvName){

	state = remove_pool(cluster,srvName);

	return ((state >= 0) ? 0 : 1);
}

/*prototipo: int create_dir(char *dirName,char *srvName)
 *objetivo: Cria um Diretorio em Servidor especificado por parametro.*/
int ss_create_dir(char *dirName,char *srvName){

	char* dir_content;
	size_t len_cont;

	dir_content = NULL;

	//aloca memoria para escrever o conteudo do arquivo
	len_cont = (sizeof(char) * strlen(dirName))+1;
	dir_content = (char *) xmalloc(len_cont);

	memset(dir_content,'\0',len_cont);

	//o conteudo do arquivo por enquanto e o nome do diretorio
	strcpy(dir_content,dirName);

	// conecta no pool/servidor
	state = set_server(cluster,srvName,&ioctx);
	if(state >= 0)//grava o arquivo sinalizador do diretorio no servidor
		state = write_object_full(ioctx, dirName, dir_content, sizeof(dir_content));
	else
		fprintf(stderr,"[create_bucket/iceph.c] Servidor não localizado!\n");

	//encerra o contexto de io
	destroy_ioctx(&ioctx);

	free(dir_content);

	return ((state >= 0) ? 0 : 1);
}

/*prototipo: int copy_dir(char *dirName,char *srvName1,char *srvName2)
 *objetivo: Copia um Diretorio de um Servidor para outro Servidor, ambos especificados por parametros.*/
int ss_copy_dir(char *dirName,char *srvName1,char *srvName2){

	/*devemos pensar como esta operacao sera realizada pois o
	 *CEPH possui o conceito de copia de Pool mas o novo diretorio
	 *nao permanece com o mesmo nome*/
	return 0;
}

/*prototipo: int drop_dir(char *dirName,char *srvName)
 *objetivo: Remove um Diretorio de um Servidor, ambos especificados por parametros.*/
int ss_drop_dir(char *dirName,char *srvName){

	/*TODO: drop dir sera mais complexo terei que criar um contexto de io no diretorio
	 *e solicitar uma lista de arquivos
	 *1) consultar no metados os buckets do diretorio (esta operacao nao existe)
	 *2) remover os buckets baseado na lista recebida
	 *3) remover arquivo correspondente ao diretorio no ceph
	 *4) remover do metadados
	 **/

	return ((state >= 0) ? 0 : 1);
}

/*prototipo: int create_bucket(char *srvName,char *dirName,char *idBucket,unsigned int maxKeys)
 *objetivo: Cria um Bucket em Diretorio especificado nos parametros de entrada, em conjunto com o
 *					 identificador do Bucket, e Servidor.*/
/*
	*header definition
	*|----------|----------|----------|----------|----------|
	 |qtdChaves | bitmap   | OFFSET#1 | LENGTH#1 | KEY#1    |
	 |----------|----------|----------|----------|----------|
	 |OFFSET#2  | LENGTH#2 | KEY#2    |   ...    | OFFSET#N |
	 |----------|----------|----------|----------|----------|
	 | LENGTH#N | KEY#N    | VALUE#1  ...  VALUE#N          |
	 |----------|----------|--------------------------------|
	*
	*max_keys = inteiro
	*bitmap = variavel
	*key (char(10)) conteudo da chave
	*offset (int) = localizacao dentro do bucket do value (deslocamento em relacao ao inicio do arquivo)
	*length = int tamanho de value
	*value (variavel) = valor do par chave-valor
	*
	*slot (key+offset+length)
*/
int ss_create_bucket(char *srvName,char *dirName,char *idBucket,unsigned int maxKeys) {

	char *header;
	unsigned char bitMap[maxKeys+1];
	unsigned int hsize,i;
	char caux[BYTES_LIMIT+1];
	size_t len_head;

	//gravacao do header

	//inicializacao do bitMap,preenche o vetor com zeros e o ultimo caracter nulo
	memset(bitMap,'\0',maxKeys+1);
	memset(bitMap,'0',maxKeys);

	//define o tamanho do header, para a criacao do bucket
	hsize = HEADER_SIZE(maxKeys);
	header = (unsigned char*) xmalloc(sizeof(unsigned char) * hsize);

	//inicializacao do header

	//converte maxKeys para char[4];
	ntochr(caux, maxKeys);

	//escreve a qtd e chaves e o mapa de bits no header
	sprintf(header,"%s%s",caux,bitMap);

	//preenche o restante do header com espacos em branco
	len_head = strlen(header);
	memset(header+len_head,' ',hsize - len_head);

	//conecta no pool/servidor
	state = set_server(cluster,srvName,&ioctx);
	if(state >= 0){
		//seta o diretorio
		state = set_directory(dirName,&ioctx);

		//grava o bucket no diretorio
		state = write_object_full(ioctx, idBucket, header, hsize);
	}else
		fprintf(stderr,"[create_bucket/iceph.c] Servidor não localizado!\n");

	free(header);

	//encerra o contexto de io
	destroy_ioctx(&ioctx);

	return ((state >= 0)? 0 : 1);
}

/*prototipo: int drop_bucket(char *idBucket,char *dirName,char *srvrName)
 *objetivo: Remove um Bucket, do Servidor e Diretório especificados por
 *parametros em conjunto com o identificador do Bucket*/
int ss_drop_bucket(char *idBucket,char *dirName,char *srvName) {

	// conecta no pool/servidor
	state = set_server(cluster, srvName,&ioctx);
	if(state >= 0){
		//seta o diretorio
		state = set_directory(dirName,&ioctx);

		/* remove o bucket do Diretorio
		   se a operacao falhar error > 0*/
		state = remove_object(ioctx,idBucket);
	}
	else
		fprintf(stderr,"[drop_bucket/iceph.c] Servidor não localizado!\n");

	destroy_ioctx(&ioctx);

	return ((state >= 0)? 0 : 1);
}

/*prototipo: int getBucket(char *srvName,char *dirName,char *idBucket)
 *objetivo: Retorna um Bucket, de um Diretorio e Servidor especificados nos parametros de entrada*/
int ss_get_bucket(char *srvName,char *dirName,char *idBucket,BUCKET_T *buff_bucket,int* hit){

	uint64_t len_bucket;
	time_t mtime;
	unsigned char *buffer;

	*buff_bucket = NULL;

	//cria um contexto de io no ceph, associando ao Pool que faz o papel de servidor
	state = set_server(cluster,srvName,&ioctx);
	if(state >= 0){
		/*se o contexto for criado seta o diretorio
		 *(associa uma chave ao contexto para influenciar o hash)*/
		state = set_directory(dirName,&ioctx);

		/*obtem o tamanho do bucket e a data de modficacao
		 *sera utilizado na leitura do bucket e para alocar memoria quando nao obtiver cache*/
		state = get_object_size(ioctx, idBucket, &len_bucket, &mtime);
	}else{
		fprintf(stderr,"[get_bucket/iceph.c] Servidor não localizado!\n");
		destroy_ioctx(&ioctx);

		return 1;
	}

	/*busca no cache o Bucket desejado, se nao encontrar
	 *requisita no sistema de armazenamento e disponibiliza no cache*/
	*hit = get_buffer(idBucket,buff_bucket);
	if(*hit == 0){
		/*se tiver cache disponivel registra o bucket no cache
		 *apos a execucao buff_bucket aponta para a area disponivel*/
		*hit = put_buffer(idBucket,"",buff_bucket);
		if(*hit == 0) //se nao tiver cache disponivel aloca individual
			*buff_bucket = (BUCKET_T) xmalloc(len_bucket);

		//busca o bucket no sistema de armazenamento
		state = read_object(ioctx,idBucket,*buff_bucket,len_bucket,0,1);
	}

  destroy_ioctx(&ioctx);

	return ( (state >= 0) ? 0 : 1);
}

/*prototipo: int is_Empty(char *idBucket,char *dirName,char *srvName)
 *objetivo: Verifica se um Bucket, especificado por parametro, esta vazio, são indicados tambem o Diretorio e Servidor
 *TODO: pensar como fazer*/
int ss_is_empty_bucket(char *idBucket,char *dirName,char *srvName){
	return 0;
}

/*prototipo: int is_Empty(char *dirName,char *srvName)
 *objetivo: Verifica se um Diretório, especificado por parametro, está vazio, deve ser indicado tambem o Servidor
 *TODO: pensar como fazer*/
int ss_is_empty_dir(char *dirName,char *srvName){
	return 0;
}

/*prototipo: int put_pair(char *srvName,char *dirName,char *idBucket,KEY_T key,char *value)
 *objetivo: adicionar um par chave-valor no bucket passado por parametro*/
int ss_put_pair(char *srvName,char *dirName,char *idBucket,KEY_T key,char *value){

	unsigned char numKeysChr[BYTES_LIMIT+1];
	unsigned char *header;
	unsigned char key_c[KEY_SLOT_SIZE];
    unsigned int hsize,numKeys,offset,len_value,slot_pos;
	int bit_pos;

	offset = slot_pos = 0;

	//conecta-se ao pool/servidor
	state	=	set_server(cluster,srvName,&ioctx);
	if(state < 0){
		fprintf(stderr,"[put_pair/iceph.c] Servidor não localizado!\n");

		return 1;
	}

	//seta o diretorio
	state	=	set_directory(dirName,&ioctx);

	/*le a qtd max de chaves para o bucket para calculo do tamanho do header
	 *a qtd de chaves e baseada no intervalo de chaves*/
	state = read_object(ioctx,idBucket,numKeysChr,BYTES_LIMIT,0,0);
	if(state < 0)
		 return 1;

  // converte o char[5] numKeysChr para inteiro -- 4bytes+1nulo
	//obtem o numero max de chaves no bucket -> baseado no intervalo de chaves
	numKeys = chrton(numKeysChr);

	// Define o tamanho do header como (qtd * tamanho da tripla) + qtd(mapabit) + NULL
	hsize = HEADER_SIZE(numKeys);
	//aloca memoria para leitura do header
	header = (unsigned char*) xmalloc(sizeof(unsigned char) * hsize);

	/*le todo o header desconsiderando os primeiros 4 bytes correspondentes
	 *a numMaxKeys que que neste caso nao sera necessario*/
	state = read_object(ioctx,idBucket,header,hsize,BYTES_LIMIT,0);
	if(state < 0){
		free(header);

		return 1;  //retorna 1 para o alocs indicando que houve falha de leitura
	}

	//length do valor do par
  len_value = strlen(value);

	//o offset comeca uma posicao apos o header
	offset = hsize;
	/*find_slot_free retorna a posicao no mapa de bits correspondente ao slot livre
	 *offset sera atualizado com o offset do slot livre*/
	bit_pos = find_slot_free(&header,numKeys,len_value,&offset);
	if(offset > 0){
		//obtem posicao do slot a partir da posicao no mapa
		slot_pos = GET_SLOT_POS(bit_pos,numKeys);

		//se encontrou um slot livre (slot_pos > 0) e o tamanho do valor não excede o valor maximo entao insere
		if( (LIMIT_SIZE_BUCKET - (offset + len_value)) >= 0 ){

			set_slot(&header,slot_pos,offset,len_value,key);

			//altera o bit correspondente ao slot no mapa para ocupado
		  header[bit_pos] = '1';

			/*atualiza o header
				*o valor sera escrito no bucket somente se a atualizacao do header for bem sucedida*/
			state = write_object(ioctx,idBucket,header,hsize,BYTES_LIMIT,0);
			if(state >= 0) // insere o valor referente a chave no bucket
				state = write_object(ioctx,idBucket,value,len_value,offset,0);
		}
	}
  else{
		//state -1 para que retorne erro
		state = -1;
		//se nao -> identificar se e bucket cheio ou limite de tamanho
		if(bit_pos < 0)
			fprintf(stderr,"[put_pair/iceph.c] Bucket Cheio!\n");
		else if( ((LIMIT_SIZE_BUCKET - (offset + len_value)) < 0 ) )
			fprintf(stderr,"[put_pair/iceph.c] O tamanho do par chave-valor excede o disponível do bucket!\n");
	}

	destroy_ioctx(&ioctx);
	free(header);

	//state retorna valor < 0 se ocorrer erro
	return ((state >= 0) ? 0 : 1);
}

/*prototipo: int rem_pair(char *srvName,char *dirName,char *idBucket,KEY_T key)
 *objetivo: Remove um par chave-valor do Bucket correspondente ao identificador passado como parametro
 *de entrada, e necessario fornecer tambem o Servidor e Diretorio*/
int ss_remove_pair(char *srvName,char *dirName,char *idBucket,KEY_T key) {

	unsigned char numKeysChr[BYTES_LIMIT+1];
	unsigned char *header,*value;
	unsigned int hsize,numKeys,slot_pos,length,offset;
	int bit_pos;

	// conecta-se ao pool/servidor
	state	=	set_server(cluster,srvName,&ioctx);
	if(state < 0){
		fprintf(stderr,"[remove_pair/iceph.c] Servidor não localizado!\n");

		return 1;
	}

	// conecta-se ao pool/servidor
	state	=	set_directory(dirName,&ioctx);

	/*recupera a qtd max de chaves do bucket para delimitar
	 * a busca no mapa de bits*/
	state = read_object(ioctx,idBucket,numKeysChr,BYTES_LIMIT,0,0);
	if(state < 0){
		destroy_ioctx(&ioctx);

		return 1;
	}

	// converte o char[5] Quantidade para inteiro
	numKeys = chrton(numKeysChr);
	//Define o tamanho do header
	hsize = HEADER_SIZE(numKeys);

	// aloca memoria para o header
	header = (unsigned char *) xmalloc(sizeof(unsigned char) * hsize);

	//obtem o header do bucket ignorando os 4 bytes iniciais que aponta a qtd max. de chaves
	state = read_object(ioctx,idBucket,header,hsize,BYTES_LIMIT,0);
	if(state < 0){
		destroy_ioctx(&ioctx);
		free(header);

		return 1;
	}

	/*percorre o mapa de bits para localizar a chave
	 *se encontrar a chave, bit_pos e atualizado com a posicao do bit no mapa
	 **/
	bit_pos = find_slot_key(&header,numKeys,key);
	if(bit_pos >= 0) {
		//obtem posicao do slot a partir da posicao no mapa
		slot_pos = GET_SLOT_POS(bit_pos,numKeys);

		//obtem o length do slot
		length = get_len_slot(header+slot_pos);
		//obtem o offset do slot
		offset = get_offset_slot(header+slot_pos);

		//muda o bit de ocupado para livre, liberando o slot
	  header[bit_pos] = '0';

		//atualiza o header no bucket
		state = write_object(ioctx,idBucket,header,hsize,BYTES_LIMIT,1);

		//aloca memoria para value
		value = (unsigned char *) xmalloc(sizeof(unsigned char) * length);

		//atualiza o valor do slot
		memset(value,' ',length);
		//atualiza o valor no bucket
		state = write_object(ioctx,idBucket,value,length,offset,0);

		free(value);
	}
	else
	  fprintf(stdout,"[remove_pair/iceph.c] Chave não encontrada\n");

	destroy_ioctx(&ioctx);
	free(header);

	return ((state >= 0) ? 0 : 1);
}
