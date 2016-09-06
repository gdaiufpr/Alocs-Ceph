
#ifndef IMDS_H
#define IMDS_H

JNIEnv* create_vm(JavaVM **jvm);

void invoke_class_createServer(JNIEnv* env, jstring path);
void invoke_class_createBucket(JNIEnv* env, jstring path, jint min, jint max);
void invoke_class_createDirectory(JNIEnv* env, jstring path);
const char * invoke_class_getKeyLocation(JNIEnv* env, jint key);
void invoke_class_deleteBucket(JNIEnv* env, jstring path);
void invoke_class_getMetadata(JNIEnv* env, jstring path);
void invoke_class_listServers(JNIEnv* env);
void invoke_class_listDirectories(JNIEnv* env, jstring path);
void invoke_class_listBuckets(JNIEnv* env, jstring path);
void invoke_class_rebalancing(JNIEnv* env);
#endif
