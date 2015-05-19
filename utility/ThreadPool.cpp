/*
 * ThreadPool.cpp
 *
 *  Created on: 2014-8-17
 *      Author: imdb
 *
 *      TODO: if task is more than thread, create some thread that execute function once
 *		可以试着添加负载均衡
 *		如果每个线程需要分配内存，为了节省内存，可以设置线程第一次运行才分配内存，优先让已经分配到内存的线程运行
 *
 */
#include "ThreadPool.h"
#include <unistd.h>
#include <sys/syscall.h>

//#define UNBLOCKED_JOIN	// cannot open, otherwise results in unknow behavior

ThreadPool::ThreadPool(){

}

ThreadPool::~ThreadPool(){
	if (thread_list_ != NULL)
		DestroyPool(this);
}

bool ThreadPool::ThreadPoolInit(int thread_count_in_pool){
	bool success = true;
	thread_count_ = thread_count_in_pool;
//	free_thread_count = thread_count;	// bug?
	free_thread_count_ = 0;
	undo_task_count_ = 0;

	pthread_mutex_init(&free_thread_count_lock_, NULL);
	pthread_mutex_init(&undo_task_count_lock_, NULL);
	pthread_mutex_init(&task_queue_lock_, NULL);

	sem_init(&undo_task_sem_, 0, 0);	// init semaphore

	thread_list_ = (pthread_t*)malloc(thread_count_in_pool * sizeof(pthread_t));
	while (!task_queue_.empty()){
		task_queue_.pop();
	}

	for (int i = 0;  i < thread_count_; ++ i) {
		if (pthread_create(&thread_list_[i], NULL, ThreadExec, this) != 0){	//if any failed, return false
			cout<<"ERROR: create pthread failed!"<<endl;
			success = false;
			break;
		}
		++free_thread_count_;
	}
	assert(free_thread_count_ == thread_count_);
	return success;
}

void ThreadPool::AddTask(Task *t){
	pthread_mutex_lock(&task_queue_lock_);
	task_queue_.push(t);
	pthread_mutex_unlock(&task_queue_lock_);

	sem_post(&undo_task_sem_);
}

void ThreadPool::AddTask(void_function f, void *a) {
	Task *task = new Task(f, a);
	AddTask(task);
}

void ThreadPool::AddDestroyTask() {
	DestroyTask *task = new DestroyTask();
	AddTask(task);
}

void ThreadPool::AddTastInSocket(void_function f, void *a, int socket_index) {
	NumaSensitiveTask *task = new NumaSensitiveTask(f, a, socket_index);
	AddTask(task);
}

void *ThreadPool::ThreadExec(void *arg){
	ThreadPool *thread_pool = (ThreadPool*)arg;
	Task *task = NULL;
	thread_pool->BindCpu();

	// every thread execute a endless loop, waiting for task, and exit when receive a task with end member of 'true'
	while (1){
		sem_wait(&(thread_pool->undo_task_sem_));

		pthread_mutex_lock(&(thread_pool->task_queue_lock_));
		if (!thread_pool->task_queue_.empty()){
			task = thread_pool->task_queue_.front();
			thread_pool->task_queue_.pop();
		}
		pthread_mutex_unlock(&(thread_pool->task_queue_lock_));

		if (task != NULL){
			ThreadPoolLogging::log("thread (id=%ld,offset=%lx) in thread pool is executing..\n", syscall(__NR_gettid), pthread_self());
			task->Run();
			ThreadPoolLogging::log("thread (id=%ld,offset=%lx) in thread pool finished executing..\n", syscall(__NR_gettid), pthread_self());
			delete task;
			task = NULL;
		}
	}
	pthread_exit(NULL);
	return NULL;
}

void ThreadPool::BindCpu(){
#ifdef UNBLOCKED_JOIN
	//将该子线程的状态设置为detached,则该线程运行结束后会自动释放所有资源,不要使父线程因为调用pthread_join而阻塞
	pthread_detach(pthread_self());
#endif

	static volatile int current_cpu = 0;
	int cpu_count = sysconf(_SC_NPROCESSORS_CONF);
	int insert_cpu = __sync_fetch_and_add(&current_cpu, 1) % cpu_count;

	int ret = setCpuAffility(insert_cpu);
	if (ret == -1){
		ThreadPoolLogging::elog("thread %ld bind cpu failed,ret = %d. %s\n", syscall(__NR_gettid), ret, strerror(errno));
	}
	else {
		ThreadPoolLogging::log("thread (tid=%ld offset=%lx) bind cpu=%ld, expect binded cpu=%ld (start=%ld end=%ld)\n",
			syscall(__NR_gettid), pthread_self(), getCurrentCpuAffility(), insert_cpu, 0, cpu_count);
	}
}

void ThreadPool::DestroyPool(ThreadPool *tp){
	//destory every thread
	for (int i = 0; i < tp->thread_count_; ++i){	// send destory task to every thread
		tp->AddDestroyTask();
	}
#ifndef UNBLOCKED_JOIN
	for (int i = 0; i < tp->thread_count_; ++i){
		pthread_join(tp->thread_list_[i], NULL);
	}
#endif
	while (!tp->task_queue_.empty()){
		Task *temp = tp->task_queue_.front();
		tp->task_queue_.pop();
		delete temp;
	}

	sem_destroy(&tp->undo_task_sem_);
	pthread_mutex_destroy(&tp->free_thread_count_lock_);
	pthread_mutex_destroy(&tp->undo_task_count_lock_);
	pthread_mutex_destroy(&tp->task_queue_lock_);

	delete tp->thread_list_;
	tp->thread_list_ = NULL;
}

