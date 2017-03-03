#include<pthread.h>
#include<stdlib.h>
#include<stdio.h>
#include<string.h>
#include "zookeeper.h"
#include "zookeeper_log.h"
#include "zlog.h"

typedef enum BOOL{true=1,false=0}bool;
typedef enum ZKErrorCode {
	ZK_SUCCESS = 0,  //操作成功,watch继续生效
	ZK_NOT_EXIT,	//节点不存在，对exist操作watch继续生效，其它操作失效
	ZK_ERROR,	//请求失败，watch失效
	ZK_DELETED,	//节点删除，watch失效
	ZK_EXISTED,	//节点已经存在，create失效
	ZK_NOT_EMPTY	//节点有子节点，delete失效
}zk_error_code;

typedef void (*session_expired_handler_f)(void* context);
typedef bool (*get_node_handler_f)(void* context);
typedef bool (*get_children_handler_f)(void* context);
typedef bool (*exist_handler_f)(void* context);
typedef bool (*create_handler_f)(void* context);
typedef bool (*set_handler_f)(void* context);
typedef bool (*delete_handler_f)(void* context);

typedef bool (*my_operation_f)(void *context);
//则里面只是概括了一些基本的操作，如果还有其他的操作，还需要进行添加

typedef enum operation_type{
	GET_NODE = 1,
	GET_CHILDREN = 2,
	CREATE_NODE = 3,
	SET = 4,
	DELETE = 5,
	EXIST = 6
	//则里面只是概括了一些基本的操作，如果还有其他的操作，还需要进行添加
}operation_type_t;

typedef union operation_handler{
	get_node_handler_f getnode_handler;
	get_children_handler_f getchildren_handler;
	exist_handler_f exist_handler;
	create_handler_f create_handler;
	set_handler_f set_handler;
	delete_handler_f delete_handler;	
}operation_handler_t;	

typedef struct operation{
	my_operation_f opt_func;
	operation_type_t opt_type;
	void *context;
	struct operation *front;
	struct operation *next;
}operation_t;

typedef struct opt_queue{
	operation_t *header;
	operation_t *end;
	int size;
}opt_queue_t;

typedef struct zk_client{
	zhandle_t *zk_handle;
	FILE *log_fp;  //zookeeper 自己维护的日志句柄；
	zlog_category_t *zlog_handle; //自己维护的zlog日志句柄
	const char *hosts;//服务器列表
	bool debug;    //设置log的级别，是debug模式？

	//ZK的会话状态
	int session_state;
	int session_timeout;
	int64_t session_disconnect_ms;
	pthread_mutex_t state_mutex;
	pthread_cond_t state_cond;

	//ZK会话状态检测线程
	bool session_check_running;
	pthread_t session_check_tid;
	
	//session expired 后的处理函数handler
	session_expired_handler_f expired_handler;
	void *expired_context;		

	//这里是operation 队列
	opt_queue_t *operation_queue;	
}zk_client_t;

typedef struct zk_watcher_context{
	bool watch;
	void * context;
	char * path;
	zk_client_t *zkclient;
	union{
		get_node_handler_f getnode_handler;
		get_children_handler_f getchildren_handler;
		exist_handler_f exit_handler;
		create_handler_f create_handler;
		set_handler_f set_handler;
		delete_handler_f delete_handler;	
	};	
}zk_watcher_context_t;

void init_zk_client(zk_client_t **zk_client,FILE *log_fp,zlog_category_t *zlog_handle,const char * hosts,bool debug,int session_time_out,session_expired_handler_f expired_handler,void *context,opt_queue_t *operation_queue);

void conn_state_watcher(zhandle_t *zh,int type,int state,const char *path,void *watcher_context);

void update_session_state(zhandle_t *zh,int type,int state,zk_client_t *zk_client);

void *session_check_thread_main(void *args);

void check_session_state(zk_client_t *zk_client);

bool myconnect(zk_client_t *zk_client);

//void default_session_expired_handler(void *context);
//int64_t get_current_ms();
bool recovery(opt_queue_t *operation_queue);
bool opt_queue_offer(opt_queue_t *queue,operation_t *node);
operation_t * opt_queue_poll(opt_queue_t *queue);
void queue_free_all(opt_queue_t *queue);
void init_myzlog(zlog_category_t **zlog_handle);
void zlog_close(zlog_category_t *zlog_handle);
