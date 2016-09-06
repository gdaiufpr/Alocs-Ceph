# alocs
API desenvolvida para explorar a alocação de dados com controle sobre a localidade, com o objetivo de
gerenciar a alocação de dados em sistemas de armazenamento distribuído, promovendo a comunicação
entre a aplicação, os sistemas de armazenamento e controle de metadados, permitindo a alocação de um conjunto de
pares chave-valor agrupados em uma única estrutura, cuja localidade é controlada pela aplicação.

A API é formada por 4 arquivos:
cache.c  funções relativas ao gerenciamento do cache
common.c concentra as funções, e tipos comuns a toda a API
libalocs.c funções relativas a interface de aplicação
iceph.c funções relativas a interface do sistema de armazenamento
imds.c funções relativas a interface do sistema de metadados
ceph_lib.c funções para comunicação com a API do sistema de armazenamento
util.c  funções úteis utilizadas por toda a API

Como utilizar a API

O comando make gera uma bibliteca compartilhada denominada libalocs que deve
ser colocada no diretório /usr/lib.
Todos os headers devem ser colocados no diretório /usr/include/alocs.

Estes procedimentos podem ser realizados através do script demonstrado abaixo.

$ ./build.sh

este script executará a sequência de comandos

$ make clean
$ make
$ sudo cp *.so.* /usr/lib/.
$ sudo cp *.h /usr/include/alocs/.


