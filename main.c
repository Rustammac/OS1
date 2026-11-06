#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <stdlib.h>

typedef struct{
	int id;
	char *message;
} Event;

typedef struct{
	Event *event;
	int event_ready;

	pthread_mutex_t mutex;
	pthread_cond_t event_available;
	pthread_cond_t slot_available;
} Monitor;

Event *event_create(int id){
	Event *event = malloc(sizeof *event);
	if(event == NULL){
		return NULL;
	}

	event->id = id;
	event->message = malloc(32);

	if(event->message == NULL){
		free(event);
		return NULL;
	}

	snprintf(event->message, 32, "payload #%d", id);

return event;
}

void event_destroy(Event *event){
	if(event == NULL){
		return;
	}
	free(event->message);
	free(event);

}


int monitor_init(Monitor *monitor){
	monitor->event_ready = 0;

	if(pthread_mutex_init(&monitor->mutex, NULL)!=0){
		return -1;
	}

	if(pthread_cond_init(&monitor->event_available, NULL)!=0){
		pthread_mutex_destroy(&monitor->mutex);
		return -1;
	}

	if(pthread_cond_init(&monitor->slot_available, NULL)!=0){
		pthread_cond_destroy(&monitor->event_available);
		pthread_mutex_destroy(&monitor->mutex);
		return -1;
	}

return 0;
}


void monitor_send(Monitor *monitor, Event *event){
	pthread_mutex_lock(&monitor->mutex);
	while(monitor->event_ready == 1){
		pthread_cond_wait(&monitor->slot_available, &monitor->mutex);
	}

	monitor->event = event;
	monitor->event_ready = 1;
	printf("Поставщик: отправил событие %d\n", event->id);
	pthread_cond_signal(&monitor->event_available);
	pthread_mutex_unlock(&monitor->mutex);
}

int monitor_receive(Monitor *monitor, Event **received_event){
	pthread_mutex_lock(&monitor->mutex);
	while(monitor->event_ready == 0){
		pthread_cond_wait(&monitor->event_available, &monitor->mutex);
	}

	*received_event = monitor->event;

	printf("Потребитель обработал событие %d - %s\n", (*received_event)->id, (*received_event)->message);

	monitor->event_ready = 0;
	pthread_cond_signal(&monitor->slot_available);
	pthread_mutex_unlock(&monitor->mutex);
	return 0;
}


// первый поток
void* producer(void* arg){
	Monitor *monitor = arg;

	for(int i = 1; i <= 5; i++){
		sleep(1);
		Event *event = event_create(i);

		if(event == NULL){
			fprintf(stderr, "Не удалось создать событие\n");
			break;
		}
		monitor_send(monitor, event);
	}
	return NULL;
}

// второй поток
void* consumer(void* arg){
	Monitor *monitor = arg;

	for(int i = 1; i <= 5; i++){
		Event *event = NULL;
		monitor_receive(monitor, &event);

		event_destroy(event);
	}
	return NULL;
}

int main(void){
	Monitor monitor;
	if(monitor_init(&monitor)!=0){
		printf("Ошибка инициализации монитора!\n");
		return -1;
	}
	pthread_t producer_thread;
	pthread_t consumer_thread;

	if(pthread_create(&producer_thread, NULL, producer, &monitor)!=0){
		pthread_cond_destroy(&monitor.event_available);
		pthread_mutex_destroy(&monitor.mutex);
		return 1;
	}

	if(pthread_create(&consumer_thread, NULL, consumer, &monitor)!=0){
		pthread_join(producer_thread, NULL);
		pthread_cond_destroy(&monitor.event_available);
		pthread_mutex_destroy(&monitor.mutex);
		return 1;
	}

	pthread_join(producer_thread, NULL);
	pthread_join(consumer_thread, NULL);
	pthread_cond_destroy(&monitor.slot_available);
	pthread_cond_destroy(&monitor.event_available);
	pthread_mutex_destroy(&monitor.mutex);

	printf("Оба потока закончили работу!!!\n");
return 0;
}
