/*
 *这是测试程序，其中自己对节点的增删查等操作函数需要自己来实现。
 *这些操作的参数都时void * 类型的，所以需要自己对函数的参数进行封装。
 */

#include<stdlib.h>
#include<stdio.h>
#include "zclient.h"
#include "zlog.h"

int main(){
	zk_client_t *client;
	zlog_category_t *zlog = NULL;
	init_myzlog(&zlog);		
	const char *zoo_log = "./zoo.log";
	FILE *log_fp = fopen(zoo_log,"w");
	const char *hosts = "localhost:2181";
	bool debug = false;
	int session_timeout = 3000;
	opt_queue_t operation_queue;
	operation_queue.header = NULL;
	operation_queue.end = NULL;

	init_zk_client(&client,log_fp,zlog,hosts,debug,session_timeout,NULL,NULL,&operation_queue);
	if(!myconnect(client))
		return -1;
	while(1){}
	return 0;
}
