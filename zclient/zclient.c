/*本程序主要是提供了zookeeper保活功能。并在session过期的情况下，
 *具有灾难现场恢复的功能
 *其中日志分为两部分：第一部分时zookeeper自带的系统日志
 *		      第二部分就是自己的一些操作日志，这里使用的时zlog
 */
#include<pthread.h>
#include<stdlib.h>
#include<stdio.h>
#include<string.h>
#include "zookeeper.h"
#include "zookeeper_log.h"
#include "zclient.h"
#include<time.h>
#include "zlog.h"

int64_t get_current_ms(){
	struct timeval tv;
	gettimeofday(&tv,NULL);
	return tv.tv_sec*1000+tv.tv_usec/1000;
}

void default_session_expired_handler(void *context){
	printf("session expired. this is the default session expired handler!");
	exit(EXIT_FAILURE);
}

void recovery_session_expired_handler(void *context){
	zk_client_t *client_h = (zk_client_t *)context;
	
	if(client_h->zk_handle){
		zookeeper_close(client_h->zk_handle);
	}
	bool con = myconnect(client_h);
	if(con == true){
		printf("re-connect: a new connect is set up");
		zlog_info(client_h->zlog_handle,"re-connect: a new connect is set up");
	}
	else{
		printf("the re-connect is failed. The server can not be connect");
		zlog_error(client_h->zlog_handle,"the re-connect is failed. The server can not be connect");
	}
	if(!(recovery(client_h->operation_queue))){
		printf("恢复现场失败");
		zlog_error(client_h->zlog_handle,"恢复现场失败");
		//这里时需要写日志的
		exit(EXIT_FAILURE);
	}
	zlog_info(client_h->zlog_handle,"恢复现场成功");
}

void init_zk_client(zk_client_t **zk_client,FILE *log_fp,zlog_category_t *zlog_handle,const char * hosts,bool debug,int session_time_out,session_expired_handler_f expired_handler,void *context,opt_queue_t *operation_queue){
	(*zk_client) = (zk_client_t*)malloc(sizeof(zk_client_t));
	(*zk_client)->zk_handle = NULL;
	(*zk_client)->log_fp = log_fp;
	(*zk_client)->zlog_handle = zlog_handle;
	(*zk_client)->hosts = hosts;
	(*zk_client)->debug = debug;
	(*zk_client)->session_state = ZOO_CONNECTING_STATE;
	(*zk_client)->session_timeout = session_time_out;
	(*zk_client)->session_disconnect_ms = 0;
	(*zk_client)->session_check_running = false;
	//(*zk_client)->session_check_tid = NULL;
	(*zk_client)->operation_queue = operation_queue;

	pthread_mutex_init(&((*zk_client)->state_mutex),NULL);
	pthread_cond_init(&((*zk_client)->state_cond),NULL);
	
	if(expired_handler == NULL){
		(*zk_client)->expired_handler = recovery_session_expired_handler;
		(*zk_client)->expired_context = (*zk_client);
	}else{
		(*zk_client)->expired_handler = expired_handler;
		(*zk_client)->expired_context = context;
	}
}

void conn_state_watcher(zhandle_t *zh,int type,int state,const char *path,void *watcher_context){
	zk_client_t *zk_client = (zk_client_t*)watcher_context;	
	update_session_state(zh,type,state,zk_client);
}

void update_session_state(zhandle_t *zh,int type,int state,zk_client_t *zk_client){
	pthread_mutex_lock(&(zk_client->state_mutex));
	zk_client->session_state = state;
	//连接建立以后，记录协商的会话过期时间，并唤醒connect函数
	if(state == ZOO_CONNECTED_STATE){
		zk_client->session_timeout = zoo_recv_timeout(zh);  
		pthread_cond_signal(&(zk_client->state_cond));
	}else if(state == ZOO_EXPIRED_SESSION_STATE){
		//会话过期，唤醒connect函数			
		pthread_cond_signal(&(zk_client->state_cond));
	}else{	//连接异常，记录下连接的开始时间，用于会话是否过期
		zk_client->session_disconnect_ms = get_current_ms();
		printf("set the session disconnect time");
	}
	pthread_mutex_unlock(&(zk_client->state_mutex));
}

void check_session_state(zk_client_t *zk_client){
	while(zk_client->session_check_running){
		bool session_expired = false;
		pthread_mutex_lock(&(zk_client->state_mutex));
		if(zk_client->session_state == ZOO_EXPIRED_SESSION_STATE){
			session_expired = true;
		}else if(zk_client->session_state != ZOO_CONNECTED_STATE){
			if(get_current_ms()-zk_client->session_disconnect_ms > zk_client->session_timeout){
				session_expired = true;
			}
		}
		
		pthread_mutex_unlock(&(zk_client->state_mutex));
		
		if(session_expired){         //如果会话过期了
			return zk_client->expired_handler(zk_client);	
		}
	}
	return;
}

void *session_check_thread_main(void *args){
	zk_client_t *zkclient = (zk_client_t *)args;
	check_session_state(zkclient);
	return NULL;
}

bool myconnect(zk_client_t *zk_client){
	//设置log级别
	ZooLogLevel log_level = zk_client->debug?ZOO_LOG_LEVEL_DEBUG:ZOO_LOG_LEVEL_INFO;
	zoo_set_debug_level(log_level);		
	if(!(zk_client->log_fp)){
		zoo_set_log_stream(zk_client->log_fp);
	}
	/*
	 *zk初始化
	 */
	zk_client->zk_handle = zookeeper_init(zk_client->hosts,conn_state_watcher,zk_client->session_timeout,0,zk_client,0);
	if(!zk_client->zk_handle){
		zlog_error(zk_client->zlog_handle,"zookeeper_init error");
		return false;
	}

	/*
	 *等待session初始化完成，有两种可能的状态：
	 *1、连接成功，会话建立
	 *2、会话过期，在初始化期间很难发生
	 */

	pthread_mutex_lock(&(zk_client->state_mutex));
	while(zk_client->session_state != ZOO_CONNECTED_STATE &&zk_client->session_state != ZOO_EXPIRED_SESSION_STATE){
		pthread_cond_wait(&(zk_client->state_cond),&(zk_client->state_mutex));
	}
	int state = zk_client->session_state;
	pthread_mutex_unlock(&(zk_client->state_mutex));
	if(state == ZOO_EXPIRED_SESSION_STATE){
		//初始建立连接的时候发生session过期
		zlog_error(zk_client->zlog_handle,"建立连接时发生session过期");
		return false;
	}
	
	zlog_info(zk_client->zlog_handle,"成功建立连接");

	/*
	 * 会话成功建立，可以启动一个zk状态检测线程，主要是预防以下两种情况的发生
	 * 1、处于session_expired状态，那么回调session_expired_handler_f，
	 *    由用户自己决定如何处理
	 * 2、处于非connected状态，那么需要判断该状态是否超过了session_timeout时间
	 *    超过则回调session_expired_handler_f
	 */
	zk_client->session_check_running = true;
	pthread_create(&(zk_client->session_check_tid),NULL,session_check_thread_main,zk_client);
	zlog_info(zk_client->zlog_handle,"成功建立状态检测线程");
	return true;
}

bool recovery(opt_queue_t *operation_queue){
	if(operation_queue->size == 0){
		return true;
	}
	operation_t *tmp = operation_queue->header;	
	while(tmp!=NULL){
		operation_type_t op_type = tmp->opt_type;
		bool ret = false;
/*****************
		switch(op_type){
			case GET_NODE: 
				ret = (tmp->operation).getnode_handler(tmp->context);
				break;
			case GET_CHILDREN:
				ret = (tmp->operation).getchildren_handler(tmp->context);
				break;
			case CREATE_NODE:
				ret = (tmp->operation).create_handler(tmp->context);
				break;
			case SET:
				ret = (tmp->operation).set_handler(tmp->context);
				break;
			case DELETE:
				ret = (tmp->operation).delete_handler(tmp->context);
				break;
			case EXIST:
				ret = (tmp->operation).exist_handler(tmp->context);
				break;
		}
**********************/
		ret = tmp->opt_func(tmp->context);
		if(ret == false){  //如果其中一个操作不能成功的话，我们就认为现场不能恢复
		//这里需要进行日志记录
			queue_free_all(operation_queue);
			return false;
		}
		tmp = tmp->next;
	}
	return true;
}

bool opt_queue_offer(opt_queue_t *queue,operation_t *node){
	if(queue->size == 0){
		queue->header = node;
		queue->end = node;
		node->front = NULL;
		node->next = NULL;
		(queue->size)++;	
		return true;
	}
	else{
		node->front = queue->end;
		node->next = NULL;
		queue->end->next = node;
		queue->end = node;
		(queue->size)++;
		return true;
	}
}
operation_t * opt_queue_poll(opt_queue_t *queue){
	if(queue->size<=0){
		printf("there is no node in the queue");
		return NULL;
	}
	else{
		operation_t *result;
		result = queue->header;
		queue->header = result->next;
		if(queue->header != NULL){
			queue->header->front = NULL;
		}
		result->next = NULL;
		(queue->size)--;
		return result;
	}
}
void queue_free_all(opt_queue_t *queue){
	while(queue->size > 0){
		operation_t *tmp = opt_queue_poll(queue);
		free(tmp->context);
		free(tmp);
	}
}

void init_myzlog(zlog_category_t **zlog_handle){
	int rc;
	rc = zlog_init("mylog.conf");
	
	if(rc){
		fprintf(stderr,"the zlog init failed!");
		exit(EXIT_FAILURE);
	}
	(*zlog_handle) = zlog_get_category("log_format");
	if((*zlog_handle) == NULL){
		fprintf(stderr,"get log format error!");
		exit(EXIT_FAILURE);
	}
}
void zlog_close(zlog_category_t *zlog_handle){
	zlog_fini();
}
