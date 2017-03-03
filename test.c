#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include "zookeeper.h"
#include "zookeeper_log.h"

void zktest_watcher_g(zhandle_t *zh,int type,int state,const char *path,void *watcherCtx){
	printf("something happend.\n");
	printf("type: %d\n",type);
	printf("state: %d\n",state);
	printf("path: %s\n",path);
	printf("watcherCtx: %s\n",(char*)watcherCtx);
	if(state != ZOO_CONNECTED_STATE)
		printf("session out time");
}
void watcher_myself(zhandle_t *zh,int type,int state,const char *path,void *watcherCtx){
	printf("just for testing\n");
	printf("path:%s\n",path);
}
void create(zhandle_t *zkhandle,char *str){
	char path_buffer[64];
	int bufferlen = sizeof(path_buffer);
	printf("同步创建节点------------------------\n");
	int flag=zoo_create(zkhandle,str,"haha",4,&ZOO_OPEN_ACL_UNSAFE,0,path_buffer,bufferlen);
	if(flag != ZOK){
		printf("节点创建失败\n");
		exit(EXIT_FAILURE);
	}
	else{
		printf("节点创建成功\n");
	}
}
void get(zhandle_t *zkhandle){
	printf("同步的方式获取节点数据--------------------\n");
	char buffer1[64];
	int bufferlen1 = sizeof(buffer1);
	int flag1 = zoo_get(zkhandle,"/xyz3000000001",0,buffer1,&bufferlen1,NULL);
	if(flag1 == ZOK)
		printf("节点/xyz300000001的数据为：%s\n",buffer1);
}

void exists(zhandle_t *zkhandle,char *str){
	int flag = zoo_exists(zkhandle,str,1,NULL);
}
void wexists(zhandle_t *zkhandle,char *str){
	int flag = zoo_wexists(zkhandle,str,watcher_myself,"test",NULL);
}

void getChildren(zhandle_t *zkhandle,char *str){
	struct String_vector strings;
	struct Stat stat;
	int flag = zoo_wget_children2(zkhandle,str,watcher_myself,"testgetChildren",&strings,&stat);
	if(flag == ZOK){
		int32_t i=0;
		for(;i<strings.count;++i){
			printf("%s\n",strings.data[i]);
		}
	}
}

void getACL(zhandle_t *zkhandle,char *str){
	struct ACL_vector acl;
	struct Stat stat;
	int flag = zoo_get_acl(zkhandle,str,&acl,&stat);
	if(flag == ZOK){
		printf("------------the ACL of %s:\n--------------",str);
		printf("%d\n",acl.count);
		printf("%d\n",acl.data->perms);
		printf("%s\n",acl.data->id.scheme);
		printf("%s\n",acl.data->id.id);
	}
}

void delete(zhandle_t *zkhandle,char *str){
	int flag = zoo_delete(zkhandle,str,-1);
	if(flag ==ZOK){
		printf("delete node success\n");
	}
}

int main(int argc,const char *argv[]){
	const char *host = "localhost:2181";
	int timeout = 3000;
	char buffer[512];
	int *bufferlen;

	zoo_set_debug_level(ZOO_LOG_LEVEL_WARN);
	zhandle_t *zkhandle = zookeeper_init(host,zktest_watcher_g,timeout,0,"hello zookeeper",0);
	if(zkhandle == NULL){
		fprintf(stderr,"error when connecting to zookeeper servers..\n");	
		exit(EXIT_FAILURE);
	}
	
	char str1[] = "/xyz3000000001";
	char str2[] = "/xyz3000000002";
	
	create(zkhandle,str2);	
	//sleep(100);
	//zookeeper_close(zkhandle);
	while(1){}
	//get(zkhandle);
	//create(zkhandle,str1);	
	//zoo_set_watcher(zkhandle,zktest_watcher_g);
	//create(zkhandle,str2);	
	//getChildren(zkhandle,str);
	//getACL(zkhandle,str);
	//delete(zkhandle,str1);
	//delete(zkhandle,str2);
}
