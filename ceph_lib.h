#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <rados/librados.h>


#ifndef CEPH_LIB_H
#define CEPH_LIB_H

//variavel que recebe o retorno das operacoes
int error;

//create cluster handle
void create_handle(rados_t *cls_handle);

//cluster shutdown
void cluster_shutdown(rados_t *cluster);

//create io context
int set_server(rados_t cluster, char *srvName,rados_ioctx_t *io_ctx);

//rados_ioctx_locator_set_key
int set_directory(char *dirName,rados_ioctx_t *io_ctx);

//destroy io context
void destroy_ioctx(rados_ioctx_t *io_ctx);

//create read operation
rados_read_op_t create_read_op(void);

//release read operation
void release_read_op(rados_read_op_t *read_op);

//create completion
int create_completion(rados_completion_t *comp);

//create pool
int create_pool(rados_t cluster, const char * pool_name, uint8_t crush_rule_num);

//get_object_size
int get_object_size(rados_ioctx_t io_ctx,char *obj_id,uint64_t *len_bucket,time_t *mtime);

//remove pool
int remove_pool(rados_t cluster, const char * pool_name);

//read data of object
int read_object(rados_ioctx_t io_ctx,char *obj_id,unsigned char *read_res,size_t len_buff,uint64_t offset,int async);

//write len bytes from buffer into object
int write_object(rados_ioctx_t io_ctx,char *obj_id,char *buffer,size_t len_buff,uint64_t offset,int async);

//rados_write_full
int write_object_full(rados_ioctx_t io_ctx, char *obj_id, char *buffer, size_t len_buff);

//remove object
int remove_object(rados_ioctx_t io_ctx,char *obj_id);

#endif
