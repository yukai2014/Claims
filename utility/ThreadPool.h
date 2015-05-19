/*
 * ThreadPool.h
 *
 *  Created on: 2014-8-17
 *      Author: imdb
 */

#ifndef THREADPOOL_H_
#define THREADPOOL_H_

#include <iostream>
#include <pthread.h>
#include <queue>
#include <semaphore.h>
#include <stdlib.h>
#include <assert.h>
#include "../common/Logging.h"
#include "Task.h"
//#define __USE_GNU		//启用CPU_ZERO等相关的宏
using namespace std;

static bool g_thread_pool_used = true;

class ThreadPool
{
public:
	typedef void *(*void_function)(void *);
public:
	ThreadPool();
	virtual ~ThreadPool();

	bool ThreadPoolInit(int thread_count_in_pool);
	static void *ThreadExec(void *arg);
	void *ThreadExecWithCond(void *arg);
	void BindCpu();

	//arg can't be a class, because delete void * won't execute destructor function
	void AddTask(void_function f, void *a);
	void AddTastInSocket(void_function f, void *a, int socket_index);
	void AddDestroyTask();
	static void DestroyPool(ThreadPool * tp);
private:
	void AddTask(Task *t);
private:
//	pthread_cond_t free_thread_cond;
//	pthread_mutex_t cond_lock;

//	attfinity_start_cpu;

	sem_t undo_task_sem_;	// undo task count

	pthread_t *thread_list_;

	queue<Task*> task_queue_;
	pthread_mutex_t task_queue_lock_;

	int thread_count_;
	int free_thread_count_;
	pthread_mutex_t free_thread_count_lock_;

	int task_count_;
	int undo_task_count_;
	pthread_mutex_t undo_task_count_lock_;

};

#endif /* THREADPOOL_H_ */
