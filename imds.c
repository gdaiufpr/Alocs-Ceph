#include <jni.h>
#include <stdio.h>
#include <stdlib.h>

JNIEnv* create_vm(JavaVM **jvm){
    JNIEnv* env;
    JavaVMInitArgs args;
    JavaVMOption options[1];
    args.version = JNI_VERSION_1_6;
    args.nOptions = 1;
    options[0].optionString = "-Djava.class.path=./:/home/ceph/Downloads/alocs/:/home/ceph/Downloads/alocs/jar/zookeeper-3.4.6.jar:/home/ceph/Downloads/alocs/jar/junit-4.11.jar:/home/ceph/Downloads/alocs/jar/log4j-1.2.16.jar:/home/ceph/Downloads/alocs/jar/slf4j-api-1.6.3.jar:/home/ceph/Downloads/alocs/jar/slf4j-log4j12-1.7.0.jar";

    args.options = options;
    args.ignoreUnrecognized = 0;

    int rv = JNI_CreateJavaVM(jvm, (void**)&env, &args);

    return env;
}

void invoke_class_createBucket(JNIEnv* env, jstring path, jint min, jint max){
    jclass call_interfacejava_class = (*env)->FindClass(env, "InterfaceJava");
    jmethodID createBucket_method = (*env)->GetStaticMethodID(env, call_interfacejava_class, "createBucket", "(Ljava/lang/String;II)V");
    (*env)->CallStaticVoidMethod(env, call_interfacejava_class, createBucket_method, path, min, max);

}

void invoke_class_createDir(JNIEnv* env, jstring path){
    jclass java_class = (*env)->FindClass(env, "InterfaceJava");
    jmethodID dir_method = (*env)->GetStaticMethodID(env, java_class, "createDirectory", "(Ljava/lang/String;)V");
    (*env)->CallStaticVoidMethod(env, java_class, dir_method, path);
}

void invoke_class_createServer(JNIEnv* env, jstring path){
    jclass java_class = (*env)->FindClass(env, "InterfaceJava");
    jmethodID java_method = (*env)->GetStaticMethodID(env,java_class,"createServer","(Ljava/lang/String;)V");
    (*env)->CallStaticVoidMethod(env, java_class, java_method, path);
}

const char * invoke_class_getKeyLocation(JNIEnv* env, jint key){
    jclass call_interfacejava_class = (*env)->FindClass(env, "InterfaceJava");
    jmethodID fetchKey_method = (*env)->GetStaticMethodID(env, call_interfacejava_class, "fetchKey", "(I)Ljava/lang/String;");
    jstring result = (*env)->CallObjectMethod(env, call_interfacejava_class, fetchKey_method, key);
    const char* cresult= (*env)->GetStringUTFChars(env, result, 0);
    printf("Location -------> %s\n" , cresult);
    return cresult;
}

void invoke_class_deleteBucket(JNIEnv* env, jstring path){
    jclass call_interfacejava_class = (*env)->FindClass(env, "javasrc/InterfaceJava");
    jmethodID deleteBucket_method = (*env)->GetStaticMethodID(env, call_interfacejava_class, "deleteBucket", "(Ljava/lang/String;)V");
    (*env)->CallStaticVoidMethod(env, call_interfacejava_class, deleteBucket_method, path);
}

void invoke_class_getMetadata(JNIEnv* env, jstring path){
    jclass call_interfacejava_class = (*env)->FindClass(env, "javasrc/InterfaceJava");
    jmethodID getMetadata_method = (*env)->GetStaticMethodID(env, call_interfacejava_class, "getMetadata", "(Ljava/lang/String;)Ljava/lang/String;");
    jstring result = (*env)->CallObjectMethod(env, call_interfacejava_class, getMetadata_method, path);
    const char* cresult= (*env)->GetStringUTFChars(env, result, 0);
    printf("Data -> %s\n" , cresult);
}

void invoke_class_listServers(JNIEnv* env){
    jclass call_interfacejava_class = (*env)->FindClass(env, "javasrc/InterfaceJava");
    jmethodID listServers_method = (*env)->GetStaticMethodID(env, call_interfacejava_class, "listServers", "()Ljava/lang/String;");
    jstring result = (*env)->CallObjectMethod(env, call_interfacejava_class, listServers_method);
    const char* cresult= (*env)->GetStringUTFChars(env, result, 0);
    printf("Servers -> %s\n" , cresult);
}

void invoke_class_listDirectories(JNIEnv* env, jstring path){
    jclass call_interfacejava_class = (*env)->FindClass(env, "javasrc/InterfaceJava");
    jmethodID listDirectories_method = (*env)->GetStaticMethodID(env, call_interfacejava_class, "listDirectories", "(Ljava/lang/String;)Ljava/lang/String;");
    jstring result = (*env)->CallObjectMethod(env, call_interfacejava_class, listDirectories_method, path);
    const char* cresult= (*env)->GetStringUTFChars(env, result, 0);
    printf("Directories -> %s\n" , cresult);
}

void invoke_class_listBuckets(JNIEnv* env, jstring path){
    jclass call_interfacejava_class = (*env)->FindClass(env, "javasrc/InterfaceJava");
    jmethodID listBuckets_method = (*env)->GetStaticMethodID(env, call_interfacejava_class, "listBuckets", "(Ljava/lang/String;)Ljava/lang/String;");
    jstring result = (*env)->CallObjectMethod(env, call_interfacejava_class, listBuckets_method, path);
    const char* cresult= (*env)->GetStringUTFChars(env, result, 0);
    printf("Buckets -> %s\n" , cresult);
}

void invoke_class_rebalancing(JNIEnv* env){
    jclass call_interfacejava_class = (*env)->FindClass(env, "javasrc/InterfaceJava");
    jmethodID rebalancing_method = (*env)->GetStaticMethodID(env, call_interfacejava_class, "rebalancing", " ()V");
    (*env)->CallObjectMethod(env, call_interfacejava_class, rebalancing_method);
}
