#define FUSE_USE_VERSION 30

#include <fuse.h>
#include <stdio.h>
#include <unistd.h>
#include <time.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <libgen.h>
#include <limits.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <arpa/inet.h>
#include <sys/xattr.h>
#include <sys/xattr.h>
#include <pthread.h>
#include <semaphore.h>
#include <time.h>
#include "parser.h"
#include "net_raid_protocol.h"
#include "cache_object.h"


struct thread_info {
  char server[256];
  char port[256];
  struct data* d;
  int sfd_num;
  int timeout;
};

struct cache_object* co;
struct cache_info* ci;
pthread_mutex_t lock;
sem_t sem;
sem_t sem1;

void cache_array_grow_capacity();
int cache_array_add_cache_object(const char* name, off_t offset, size_t size, const char* content, int eq);
int cache_array_contains_cache_object(const char* name, off_t offset, size_t size, int eq);
int cache_array_remove_cache_object(const char* name, off_t offset, size_t size, int eq);
int cache_array_remove_first_cache_object();
void cache_array_change_cache_object(const char* name, off_t offset, size_t size, const char* new_name, int eq);
struct cache_object* cache_array_get_cache_object(const char* name, off_t offset, size_t size, int eq);
void cache_array_change_cache_object_name(const char* name, const char* new_name);
void cache_array_change_cache_object_status(const char* name, off_t offset, size_t size, int eq);


void recover_file(int to_sfd, int from_sfd, char* path);
void check_hash(char old_buf0[33], char new_buf0[33], char old_buf1[33], char new_buf1[33],
                      char* path, struct data* d, int sfd_num1, int sfd_num2);
void copy_data(int to_sfd_num, int from_sfd_num, struct data* d);

int netRaid_getattr(const char *path, struct stat *statbuf);
int netRaid_opendir(const char *path, struct fuse_file_info *fi);
int netRaid_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi);
int netRaid_mknod(const char *path, mode_t mode, dev_t dev);
int netRaid_mkdir(const char *path, mode_t mode);
int netRaid_rename(const char *path, const char *newpath);
int netRaid_rmdir(const char *path);
int netRaid_unlink(const char *path);
int netRaid_open(const char *path, struct fuse_file_info *fi);
int netRaid_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi);
int netRaid_releasedir(const char *path, struct fuse_file_info *fi);
int netRaid_release(const char *path, struct fuse_file_info *fi);
int netRaid_truncate(const char *path, off_t newsize);
int netRaid_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi);

int get_worker_sfd_num(struct data * d);
int get_second_worker_sfd_num(struct data * d);

void get_time(char buffer[26]);
int connect_to_server(char* server, char* port, struct data* d, int sfd_num, int new_sfd, int b);
void* run_fuse_thread(void* thread_info_data);
void control_server(char* server, char* port, struct data* d, int sfd_num);
void* run_infinite_thread(void* thread_info_data);
void runClient(char* server, char* port, struct data* d, int sfd_num);
void parseServerInfo(char* info, char* server, char* port);


void cache_array_grow_capacity() {
  //printf("%s\n", "capacity growth");
  ci->capacity *= 2;
  co = realloc(co, ci->capacity * sizeof(struct cache_object));
}

int cache_array_contains_cache_object(const char* name, off_t offset, size_t size, int eq) {
  int result = -1;
  int i = 0;
  if(eq == 0) { // We need file just by a name.
    for(i = 0; i < ci->num_elems; i++) {
      if(strcmp(name, co[i].name) == 0) {
        result = i;
        break;
      }
    }
  }
  if(eq == 1) { // By file name and offset
    for(i = 0; i < ci->num_elems; i++) {
      if(strcmp(name, co[i].name) == 0 && co[i].offset == offset) {
        result = i;
        break;
      }
    }
  }
  if(eq == 2) { // 
    for(i = 0; i < ci->num_elems; i++) {
      if(strcmp(name, co[i].name) == 0 && offset < co[i].offset + co[i].size) {
        result = i;
        break;
      }
    }
  }
  if(eq == 3) { // File name da amaze meti offset
    for(i = 0; i < ci->num_elems; i++) {
      if(strcmp(name, co[i].name) == 0 && co[i].offset >= offset) {
        result = i;
        break;
      }
    }
  }
  if(eq == 4) { // File name, iseti offseti da iseti size, rom gadmocemuli offseti + gadmocemuli
                // size motavsdeds masshi.
    for(i = 0; i < ci->num_elems; i++) {
      if(strcmp(name, co[i].name) == 0 && co[i].offset <= offset && co[i].offset + co[i].size >= offset + size) {
        result = i;
        break;
      }
    }
  }
  if(eq == 5) { // Emtxveva name, offset da zoma gadmocemulze didia
    for(i = 0; i < ci->num_elems; i++) {
      if(strcmp(name, co[i].name) == 0 && co[i].offset == offset && co[i].size >= size) {
        result = i;
        break;
      }
    }
  }
  if(eq == 5) { // Emtxveva saxelic, offsetic da zomac
    for(i = 0; i < ci->num_elems; i++) {
      if(strcmp(name, co[i].name) == 0 && co[i].offset == offset && co[i].size == size) {
        result = i;
        break;
      }
    }
  }
  return result;
}

int cache_array_add_cache_object(const char* name, off_t offset, size_t size, const char* content, int eq) {
  int result = 0;
  int p = 0;
  while(1) {
    p = cache_array_remove_cache_object(name, offset, size, eq);
    if(p == -1) break;
  }
  if(size > ci->max_size) { // blokis zoma qeshis zomaze didia.
    //printf("%s\n", "Too big file, can't put it in cahche");
    return -1;
  }
  while(ci->cur_size + size > ci->max_size) { // blokis zoma didia, chaeteva qesshi tu gamovutavisufleb adgils.
    //printf("%s\n", "No place for this file in cahche, need to remove other files.");
    int k = cache_array_remove_first_cache_object();
    if(k == -1) break;
  }


  if(ci->num_elems + 1 == ci->capacity) { // masivis zrdaa sawiro
    cache_array_grow_capacity();
  }
  int i = ci->num_elems;
  struct cache_object* new_object = malloc(sizeof(struct cache_object));
  new_object->offset = offset;
  new_object->size = size;
  memcpy(new_object->name, name, strlen(name));
  new_object->name[strlen(name)] = '\0';
  char* empty = strdup("");
  new_object->content = malloc(size + 1);
  memcpy(new_object->content, content, size);
  strcpy(new_object->content + size, empty);
  memcpy((char*)co + i * sizeof(struct cache_object), new_object, sizeof(struct cache_object));
  free(new_object);
  ci->num_elems++;
  ci->cur_size += size;
  //printf("%s%s\n", "added new file in cache, path: ", name);
  return result;
}

/*
  gadmocemuli moudit vpoulob cache_objects da vshli. 0 on succes, -1 tu araa aseti obieqti qeshshi
*/
int cache_array_remove_cache_object(const char* name, off_t offset, size_t size, int eq) {
  int pos = cache_array_contains_cache_object(name, offset, size, eq);
  if(pos == -1) {
    return - 1;
  }
  struct cache_object* ro = (struct cache_object*)((char*)co + pos * sizeof(struct cache_object));
  ci->num_elems--;
  ci->cur_size -= ro->size;
  memmove((char*)co + pos * sizeof(struct cache_object),
      (char*)co + (pos + 1) * sizeof(struct cache_object),
      (ci->num_elems - pos) * (sizeof(struct cache_object)));
  //printf("%s%s\n", "removed file from a cache, path: ", name);
  return 0;
}

/*
  yvelaze dzveli blokis gamodzeveba
*/
int cache_array_remove_first_cache_object() {
  int pos = 0;
  if(ci->num_elems == 0) {
    //printf("%s\n", "Can not remove any files. because cache is empty.");
    return -1;
  }
  struct cache_object* ro = (struct cache_object*)((char*)co + pos * sizeof(struct cache_object));
  ci->num_elems--;
  ci->cur_size -= ro->size;
  char* path = ro->name;
  //printf("%s%s\n", "removed file, path: ", path);
  memmove(co,
      (char*)co + sizeof(struct cache_object),
      ci->num_elems * sizeof(struct cache_object)); 
  return 0;
}

/*
  gadmocemuli infoti da moudit brundeba cache_object
*/
struct cache_object* cache_array_get_cache_object(const char* name, off_t offset, size_t size, int eq) {
  int pos = cache_array_contains_cache_object(name, offset, size, eq);
  if(pos == -1) {
    return NULL;
  }
  struct cache_object* ro = (struct cache_object*)((char*)co + pos * sizeof(struct cache_object));
  return ro;
}


// saxelis shecvla
void cache_array_change_cache_object_name(const char* name, const char* new_name) {
  int pos = cache_array_contains_cache_object(name, 0, 0, 0);
  while(pos != -1) {
    if(pos == -1) {
      return;
    }
    struct cache_object* c = (struct cache_object*)((char*)co + pos * sizeof(struct cache_object));
    memcpy(c->name, new_name, strlen(new_name));
    c->name[strlen(new_name)] = '\0';
    //printf("%s%s%s%s\n", "Renamed file in a cache, old path: ", name, " new path: ", new_name);
    pos = cache_array_contains_cache_object(name, 0, 0, 0);
  }
}

// Tu bloki maqvs qesshi da momxmarebeli waikitxavs mas, vaapdeiteb blokis prioritets
void cache_array_change_cache_object_status(const char* name, off_t offset, size_t size, int eq) {
  int pos = cache_array_contains_cache_object(name, offset, size, eq);
  if(pos == -1) {
    //printf("%s%s\n", "No such file in cache: ", name);
  }
  struct cache_object* c = (struct cache_object*)((char*)co + pos * sizeof(struct cache_object));
  memcpy((char*)co + ci->num_elems * sizeof(struct cache_object), c, sizeof(struct cache_object));
  memmove((char*)co + pos * sizeof(struct cache_object),
          (char*)co + (pos + 1) * sizeof(struct cache_object),
          (ci->num_elems - pos) * sizeof(struct cache_object));
  //printf("%s\n", "Status changed.");
}

void recover_file(int to_sfd, int from_sfd, char* path) {
  off_t offset = 0;
  size_t size = 8192;
  while(1) {
    char buf[8192];
    struct protocol p_read;
    p_read.function_number = READ;
    p_read.size = size;
    p_read.offset = offset;
    strcpy(p_read.path, path);
    int sz = send(from_sfd, &p_read, sizeof(struct protocol), 0);
    //printf("%s%d\n", "sz   ", sz);
    int result_read = 0;
    recv(from_sfd, &result_read, sizeof(int), 0);
    recv(from_sfd, buf, size, 0);
    //printf("%s%d\n", "result read ", result_read);
    //printf("%s\n", buf);
    if(result_read == 0) {
      break;
    }
    struct protocol p_write;
    p_write.function_number = WRITE;
    strcpy(p_write.path, path);
    p_write.size = result_read;
    p_write.offset = offset;
    p_write.recover = 1;
    if(offset == 0) {
      p_write.append = 0;
    } else {
      p_write.append = 1;
    }
    //printf("%s\n", "modis");
  	//printf("%s\n", );

  	struct protocol p_tr;
  	p_tr.function_number = TRUNCATE;
  	p_tr.newsize = offset + result_read;
  	strcpy(p_tr.path, path);
  	send(to_sfd, &p_tr, sizeof(struct protocol), 0);
  	int result_tr;
  	size_t recv_size = recv(to_sfd, &result_tr, sizeof(int), 0);
  	//printf("%d\n", result_tr);

    send(to_sfd, &p_write, sizeof(struct protocol), 0);  
    send(to_sfd, buf, result_read, 0);
    int result_write;
    recv(to_sfd, &result_write, sizeof(int), 0);
    //printf("%s%d\n", "result write ", result_write);
    offset += result_read;
    if(result_read < size) {
      break;
    }
  }
}


void check_hash(char old_buf0[33], char new_buf0[33], char old_buf1[33],
  char new_buf1[33], char* path, struct data* d, int sfd_num1, int sfd_num2) {
  if(strcmp(old_buf0, new_buf0) == 0 && strcmp(old_buf1, new_buf1) == 0) {
    // Do nothing
  } else if(strcmp(old_buf0, new_buf0) == 0 && strcmp(old_buf1, new_buf1) != 0) {
    // From second to first
    recover_file(d->sfd[sfd_num2], d->sfd[sfd_num1], path);
    fprintf(d->errorlog_fptr, "\n%s%s%s%s%s%s",
                    "recover file from ", d->server_names[sfd_num1],
                    " to ", d->server_names[sfd_num2],
                    ". file path: ", path);
  } else if(strcmp(old_buf0, new_buf0) != 0 && strcmp(old_buf1, new_buf1) == 0) {
    // From first to second
    recover_file(d->sfd[sfd_num1], d->sfd[sfd_num2], path);
    fprintf(d->errorlog_fptr, "\n%s%s%s%s%s%s",
                    "recover file from ", d->server_names[sfd_num2],
                    " to ", d->server_names[sfd_num1],
                    ". file path: ", path);
  } else if(strcmp(old_buf0, new_buf0) != 0 && strcmp(old_buf1, new_buf1) != 0) {
    // can't recover, delete!
    struct protocol p_unlink;
    p_unlink.function_number = UNLINK;
    strcpy(p_unlink.path, path);
    send(d->sfd[sfd_num1], &p_unlink, sizeof(struct protocol), 0);
    int result_unlink;
    recv(d->sfd[sfd_num1], &result_unlink, sizeof(int), 0);
    send(d->sfd[sfd_num2], &p_unlink, sizeof(struct protocol), 0);
    recv(d->sfd[sfd_num2], &result_unlink, sizeof(int), 0);
    fprintf(d->errorlog_fptr, "\n%s%s", "File is damaged on both servers: ", path);
  }
}

void copy_data(int to_sfd_num, int from_sfd_num, struct data* d) {
  pthread_mutex_lock(&lock);
  struct protocol p;
  p.function_number = FILE_PATH;
  size_t send_size = send(d->sfd[from_sfd_num], &p, sizeof(struct protocol), 0);
  int size;
  size_t recv_size = recv(d->sfd[from_sfd_num], &size, sizeof(int), 0);
  fixed_size_path fsp_list[size];
  int i = 0;
  int j = 0;
  while(1) {
    if(i == size) {
      break;
    }
    fixed_size_path fsp;
    int md;
    recv(d->sfd[from_sfd_num], &fsp, PATH_MAX + 1, 0);
    recv(d->sfd[from_sfd_num], &md, sizeof(int), 0);
    if(fsp[0] == 'd' && strlen(fsp) > 1) {
      struct protocol p_mkdir;
      p_mkdir.function_number = MKDIR;
      strcpy(p_mkdir.path, fsp + 1);
      p_mkdir.mode = md;
      send_size = send(d->sfd[to_sfd_num], &p_mkdir, sizeof(struct protocol), 0);
      int result = -1;
      recv_size = recv(d->sfd[to_sfd_num], &result, sizeof(int), 0);
      //printf("%d\n", result);
    } else if(fsp[0] == 'f' && strlen(fsp) > 1) {
      struct protocol p_mknod;
      p_mknod.function_number = MKNOD;
      strcpy(p_mknod.path, fsp + 1);
      p_mknod.mode = md;
      //printf("%d\n", md);
      send_size = send(d->sfd[to_sfd_num], &p_mknod, sizeof(struct protocol), 0);
      int result = -errno;
      recv_size = recv(d->sfd[to_sfd_num], &result, sizeof(int), 0);
      strcpy(fsp_list[j], p_mknod.path);
      j++;
    }
    i++;
  }
  //printf("%s\n", "aaaaaaaaaa");
  int k;
  for(k = 0; k < j; k++) {
    //printf("%s\n", fsp_list[k]);
    recover_file(d->sfd[to_sfd_num], d->sfd[from_sfd_num], fsp_list[k]);
  }
    //printf("%s\n", "bbbbbbbbb");
  pthread_mutex_unlock(&lock);
  sem_post(&sem1);
}

int get_worker_sfd_num(struct data * d) {
  int result = -1;
  int i;
  for(i = 0; i < 3; i++) {
    if(d->sfd_status[i] == 1) {
      result = i;
      break;
    }
  }
  if(result == 2 && d->hotswap_status == 0) {
    result = -1;
  }
  return result;
}

int get_second_worker_sfd_num(struct data * d) {
  int result = -1;
  int count = 0;
  int i;
  for(i = 0; i < 3; i++) {
    if(d->sfd_status[i] == 1) {
      count++;
      if(count == 2) {
        result = i;
        break;  
      }
    }
  }
  if(result == 2 && d->hotswap_status == 0) {
    result = -1;
  }
  return result;
}


int netRaid_getattr(const char *path, struct stat *statbuf) {
  pthread_mutex_lock(&lock);
  struct data* d  = (struct data*) fuse_get_context()->private_data;
  struct protocol p;
  p.function_number = GETATTR;
  strcpy(p.path, path);
  int sn = get_worker_sfd_num(d);
  if(sn == -1) {
  	printf("%s\n", "There are no working servers.");
  	pthread_mutex_unlock(&lock);
    return -errno;
  }
  size_t send_size = send(d->sfd[sn], &p, sizeof(struct protocol), 0);
  struct getattr_protocol gp;
  size_t recv_size = recv(d->sfd[sn], &gp, sizeof(struct getattr_protocol), 0);
  memcpy(statbuf, &(gp.sb), sizeof(struct stat));
  pthread_mutex_unlock(&lock);
  return gp.status;
}


int netRaid_opendir(const char *path, struct fuse_file_info *fi) {
  pthread_mutex_lock(&lock);
  int result;
  struct data* d  = (struct data*) fuse_get_context()->private_data;
  struct protocol p;
  p.function_number = OPENDIR;
  strcpy(p.path, path);
  int sn = get_worker_sfd_num(d);
  if(sn == -1) {
  	printf("%s\n", "There are no working servers.");
  	pthread_mutex_unlock(&lock);
    return -errno;
  }
  size_t send_size = send(d->sfd[sn], &p, sizeof(struct protocol), 0);
  struct opendir_protocol op;
  size_t recv_size = recv(d->sfd[sn], &op, sizeof(struct opendir_protocol), 0);
  fi->fh = (intptr_t)(op.dp);
  result = op.status;
  pthread_mutex_unlock(&lock);
  return result;
}


int netRaid_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {
  pthread_mutex_lock(&lock);
  struct data* d  = (struct data*) fuse_get_context()->private_data;
  struct protocol p;
  p.function_number = READDIR;
  strcpy(p.path, path);
  p.offset = offset;
  int sn = get_worker_sfd_num(d);
  if(sn == -1) {
  	printf("%s\n", "There are no working servers.");
  	pthread_mutex_unlock(&lock);
    return -errno;
  }
  size_t send_size = send(d->sfd[sn], &p, sizeof(struct protocol), 0);
  size_t size;
  int result;
  size_t recv_size1 = recv(d->sfd[sn], &size, sizeof(size_t), 0);
  char str[size];
  size_t recv_size2 = recv(d->sfd[sn], str, size, 0);
  size_t recv_size3 = recv(d->sfd[sn], &result, sizeof(int), 0);
  char* token = strtok(str, "/");
  while(1) {
    if(token == NULL) break;
    if(filler(buf, token, NULL, 0) != 0) {
      pthread_mutex_unlock(&lock);
      return -ENOMEM;
    }
    token = strtok(NULL, "/");
  }
  pthread_mutex_unlock(&lock);
  return result;
}


int netRaid_mknod(const char *path, mode_t mode, dev_t dev) {
  pthread_mutex_lock(&lock);
  struct data* d  = (struct data*) fuse_get_context()->private_data;
  struct protocol p;
  p.function_number = MKNOD;
  strcpy(p.path, path);
  p.mode = mode;
  int sn = get_worker_sfd_num(d);
  if(sn == -1 || (d->num_of_working_servers == 1 && d->num_of_healthy_servers > 1)) {
    pthread_mutex_unlock(&lock);
    return -errno;
  } 
  size_t send_size = send(d->sfd[sn], &p, sizeof(struct protocol), 0);
  int result = -errno;
  size_t recv_size = recv(d->sfd[sn], &result, sizeof(int), 0);
  int res_sec_ser = -errno;
  int sn2 = get_second_worker_sfd_num(d);
  if(sn2 != -1 && result >= 0) {
    send_size = send(d->sfd[sn2], &p, sizeof(struct protocol), 0);  
    recv_size = recv(d->sfd[sn2], &res_sec_ser, sizeof(int), 0);
  } else {
    result = -errno;
  }
  pthread_mutex_unlock(&lock);
  return result;
}


int netRaid_mkdir(const char *path, mode_t mode) {
  pthread_mutex_lock(&lock);
  struct data* d  = (struct data*) fuse_get_context()->private_data;
  struct protocol p;
  p.function_number = MKDIR;
  strcpy(p.path, path);
  p.mode = mode;
  int sn = get_worker_sfd_num(d);
  if(sn == -1 || (d->num_of_working_servers == 1 && d->num_of_healthy_servers > 1)) {
    pthread_mutex_unlock(&lock);
    return -errno;
  }
  size_t send_size = send(d->sfd[sn], &p, sizeof(struct protocol), 0);
  int result = -1;
  size_t recv_size = recv(d->sfd[sn], &result, sizeof(int), 0);
  int res_sec_ser = -1;
  int sn2 = get_second_worker_sfd_num(d);
  if(sn2 != -1 && result >= 0) {
    send_size = send(d->sfd[sn2], &p, sizeof(struct protocol), 0);
    recv_size = recv(d->sfd[sn2], &res_sec_ser, sizeof(int), 0);
  } else {
    result = -errno;
  }
  pthread_mutex_unlock(&lock);
  return result; 
}


int netRaid_rename(const char *path, const char *newpath) {
  pthread_mutex_lock(&lock);
  struct cache_object* c = cache_array_get_cache_object(path, 0, 0, 0);
  if(c != NULL) {
    cache_array_change_cache_object_name(path, newpath);
  }
  struct data* d  = (struct data*) fuse_get_context()->private_data;
  struct protocol p;
  p.function_number = RENAME;
  strcpy(p.path, path);
  strcpy(p.newpath, newpath);
  int sn = get_worker_sfd_num(d);
  if(sn == -1 || (d->num_of_working_servers == 1 && d->num_of_healthy_servers > 1)) {
    pthread_mutex_unlock(&lock);
    return -errno;
  }
  size_t send_size = send(d->sfd[sn], &p, sizeof(struct protocol), 0);
  int result = -errno;
  size_t recv_size = recv(d->sfd[sn], &result, sizeof(int), 0);
  int res_sec_ser = -errno;
  int sn2 = get_second_worker_sfd_num(d);
  if(sn2 != -1 && result >= 0) {
    send_size = send(d->sfd[sn2], &p, sizeof(struct protocol), 0);
    recv_size = recv(d->sfd[sn2], &res_sec_ser, sizeof(int), 0);
  } else {
    result = -errno;
  }
  pthread_mutex_unlock(&lock);
  return result;
}


int netRaid_rmdir(const char *path) {
  pthread_mutex_lock(&lock);
  struct data* d  = (struct data*) fuse_get_context()->private_data;
  struct protocol p;
  p.function_number = RMDIR;
  strcpy(p.path, path);
  int sn = get_worker_sfd_num(d);
  if(sn == -1 || (d->num_of_working_servers == 1 && d->num_of_healthy_servers > 1)) {
    pthread_mutex_unlock(&lock);
    return -errno;
  }
  size_t send_size = send(d->sfd[sn], &p, sizeof(struct protocol), 0);
  int result = -errno;
  size_t recv_size = recv(d->sfd[sn], &result, sizeof(int), 0);
  int res_sec_ser = -errno;
  int sn2 = get_second_worker_sfd_num(d);
  if(sn2 != -1 && result >= 0) {
    send_size  = send(d->sfd[sn2], &p, sizeof(struct protocol), 0);
    recv_size = recv(d->sfd[sn2], &res_sec_ser, sizeof(int), 0);
  } else {
    result = -errno;
  }
  pthread_mutex_unlock(&lock);
  return result;
}


int netRaid_unlink(const char *path) {
  pthread_mutex_lock(&lock);
  while(1) {
    int r = cache_array_remove_cache_object(path, 0, 0, 0);
    if(r == -1) {
      break;
    }
  }
  struct data* d  = (struct data*) fuse_get_context()->private_data;
  struct protocol p;
  p.function_number = UNLINK;
  strcpy(p.path, path);
  int sn = get_worker_sfd_num(d);
  if(sn == -1 || (d->num_of_working_servers == 1 && d->num_of_healthy_servers > 1)) {
    pthread_mutex_unlock(&lock);
    return -errno;
  } 
  size_t send_size = send(d->sfd[sn], &p, sizeof(struct protocol), 0);
  int result = -errno;
  size_t recv_size = recv(d->sfd[sn], &result, sizeof(int), 0);
  int res_sec_ser = -errno;
  int sn2 = get_second_worker_sfd_num(d);
  if(sn2 != -1 && result >= 0) {
    send_size = send(d->sfd[sn2], &p, sizeof(struct protocol), 0);
    recv_size = recv(d->sfd[sn2], &res_sec_ser, sizeof(int), 0);
  } else {
    result = -errno;
  }
  pthread_mutex_unlock(&lock);
  return result;
}


int netRaid_open(const char *path, struct fuse_file_info *fi) {
  pthread_mutex_lock(&lock);
  int result = -errno;
  struct data* d  = (struct data*) fuse_get_context()->private_data;
  struct protocol p;
  p.function_number = OPEN;
  p.flags = fi->flags;
  strcpy(p.path, path);
  int sn = get_worker_sfd_num(d);
  if(sn == -1) {
    pthread_mutex_unlock(&lock);
    return -errno;
  }  
  size_t send_size = send(d->sfd[sn], &p, sizeof(struct protocol), 0);
  struct open_protocol op;
  size_t recv_size = recv(d->sfd[sn], &op, sizeof(struct open_protocol), 0);
  char old_buf0[33];
  char new_buf0[33];
  recv_size = recv(d->sfd[sn], old_buf0, 33, 0);
  recv_size = recv(d->sfd[sn], new_buf0, 33, 0);
  result = op.status;
  fi->fh = op.fd;
  int res_sec_ser = -errno;
  int sn2 = get_second_worker_sfd_num(d);
  if(sn2 != -1) {
    send_size = send(d->sfd[sn2], &p, sizeof(struct protocol), 0);
    struct open_protocol op_sec_ser;
    recv_size = recv(d->sfd[sn2], &op_sec_ser, sizeof(struct open_protocol), 0);
    char old_buf1[33];
    char new_buf1[33];
    recv_size = recv(d->sfd[sn2], old_buf1, 33, 0);
    recv_size = recv(d->sfd[sn2], new_buf1, 33, 0);
    check_hash(old_buf0, new_buf0, old_buf1, new_buf1, p.path, d, sn, sn2);
  }
  pthread_mutex_unlock(&lock);
  return result;
}


int netRaid_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
  pthread_mutex_lock(&lock);
  int result = 0;
  struct cache_object* c = cache_array_get_cache_object(path, offset, size, 4);
  if(c != NULL) {
    if(offset < c->size) {
      //printf("%s%s\n", "Reading from a cache, path ", path);
      off_t o = offset - c->offset;
      result = c->size;
      memcpy(buf, c->content + o, size);
      if(result < size) {
        result = 0;
      }
      cache_array_change_cache_object_status(path, c->offset, c->size, 5);
    } else {
      result = 0;
    }
     pthread_mutex_unlock(&lock);
     return result;
  }
  struct data* d  = (struct data*) fuse_get_context()->private_data;
  struct protocol p;
  p.function_number = READ;
  p.size = size;
  p.offset = offset;
  strcpy(p.path, path);
  int sn = get_worker_sfd_num(d);
  if(sn == -1) {
    pthread_mutex_unlock(&lock);
    return -errno;
  }
  size_t send_size = send(d->sfd[sn], &p, sizeof(struct protocol), 0);
  size_t recv_size = recv(d->sfd[sn], &result, sizeof(int), 0);
  char tmp_buf[size];
  recv_size = recv(d->sfd[sn], tmp_buf, size, 0);
  memcpy(buf, tmp_buf, size);
  if(c == NULL) {
    //printf("%s%s\n", "Writing in a cache, path ", path);
    cache_array_add_cache_object(path, offset, size, tmp_buf, -1);
  }
  pthread_mutex_unlock(&lock);
  return result;
}


int netRaid_releasedir(const char *path, struct fuse_file_info *fi) {
  pthread_mutex_lock(&lock);
  struct data* d  = (struct data*) fuse_get_context()->private_data;
  struct protocol p;
  p.function_number = RELEASEDIR;
  strcpy(p.path, path);
  int sn = get_worker_sfd_num(d);
  if(sn == -1) {
  	printf("%s\n", "There are no working servers.");
  	pthread_mutex_unlock(&lock);
    return -errno;
  }
  size_t send_size = send(d->sfd[sn], &p, sizeof(struct protocol), 0);  
  int result = -errno;
  size_t recv_size = recv(d->sfd[sn], &result, sizeof(int), 0);
  pthread_mutex_unlock(&lock);
  return result;
}


int netRaid_release(const char *path, struct fuse_file_info *fi) {//+
  pthread_mutex_lock(&lock);
  struct data* d  = (struct data*) fuse_get_context()->private_data;
  struct protocol p;
  p.function_number = RELEASE;
  int sn = get_worker_sfd_num(d);
  if(sn == -1) {
    pthread_mutex_unlock(&lock);
    return -errno;
  }
  size_t send_size = send(d->sfd[sn], &p, sizeof(struct protocol), 0);
  int result = -errno;
  size_t recv_size = recv(d->sfd[sn], &result, sizeof(int), 0);
  int res_sec_ser = -errno;
  int sn2 = get_second_worker_sfd_num(d);
  if(sn2 != -1) {
    struct protocol p_sec_ser;
    p_sec_ser.function_number = RELEASE;
    send_size = send(d->sfd[sn2], &p_sec_ser, sizeof(struct protocol), 0);
    recv_size = recv(d->sfd[sn2], &res_sec_ser, sizeof(int), 0);
  }
  pthread_mutex_unlock(&lock);
  return result;
}


int netRaid_truncate(const char *path, off_t newsize) {
  pthread_mutex_lock(&lock);
  struct data* d  = (struct data*) fuse_get_context()->private_data;
  struct protocol p;
  p.function_number = TRUNCATE;
  p.newsize = newsize;
  strcpy(p.path, path);
  int sn = get_worker_sfd_num(d);
  if(sn == -1) {
    pthread_mutex_unlock(&lock);
    return -errno;
  }
  size_t send_size = send(d->sfd[sn], &p, sizeof(struct protocol), 0);
  int result = -errno;
  size_t recv_size = recv(d->sfd[sn], &result, sizeof(int), 0);
  int res_sec_ser = -errno;
  int sn2 = get_second_worker_sfd_num(d);
  if(sn2 != -1) {
    int res_sec_ser;
    send_size = send(d->sfd[sn2], &p, sizeof(struct protocol), 0);
    recv_size = recv(d->sfd[sn2], &res_sec_ser, sizeof(int), 0);
  }
  pthread_mutex_unlock(&lock);
  return result; 
}


int netRaid_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {//
  pthread_mutex_lock(&lock);
  int pp = 0;
  while(1) {
    pp = cache_array_remove_cache_object(path, offset, size, 0);
    if(pp == -1) break;
  }
  struct data* d  = (struct data*) fuse_get_context()->private_data;
  struct protocol p;
  p.function_number = WRITE;
  strcpy(p.path, path);
  p.size = size;
  p.offset = offset;
  p.recover = 0;
  int result;
  int sn = get_worker_sfd_num(d);
  if(sn == -1) {
    pthread_mutex_unlock(&lock);
    return -errno;
  }
  send(d->sfd[sn], &p, sizeof(struct protocol), 0);  
  send(d->sfd[sn], buf, size, 0);
  recv(d->sfd[sn], &result, sizeof(int), 0);
  int res_sec_ser = -errno;
  int sn2 = get_second_worker_sfd_num(d);
  if(sn2 != -1 && result >= 0) {
    struct protocol p_sec_ser;
    p_sec_ser.function_number = WRITE;
    strcpy(p_sec_ser.path, path);
    p_sec_ser.size = size;
    p_sec_ser.offset = offset;
    p_sec_ser.recover = 0;
    send(d->sfd[sn2], &p_sec_ser, sizeof(struct protocol), 0);
    send(d->sfd[sn2], buf, size, 0);
    recv(d->sfd[sn2], &res_sec_ser, sizeof(int), 0);
  }
  pthread_mutex_unlock(&lock);
  return result; 
}


struct fuse_operations netRaid_oper = {
  .getattr = netRaid_getattr,
  .opendir = netRaid_opendir,
  .readdir = netRaid_readdir,
  .mknod = netRaid_mknod,
  .mkdir = netRaid_mkdir,
  .rename = netRaid_rename,
  .rmdir = netRaid_rmdir,
  .unlink = netRaid_unlink,
  .open = netRaid_open,
  .read = netRaid_read,
  .releasedir = netRaid_releasedir,
  .release = netRaid_release,
  .truncate = netRaid_truncate,
  .write = netRaid_write,
};


int connect_to_server(char* server, char* port, struct data* d, int sfd_num, int new_sfd, int b) {
  int result;
  struct sockaddr_in addr;
  int ip;
  inet_pton(AF_INET, server, &ip);
  int portNum = atoi(port);
  addr.sin_family = AF_INET;
  addr.sin_port = htons(portNum);
  addr.sin_addr.s_addr = ip;
  if(b == 0) {
    result = connect(d->sfd[sfd_num], (struct sockaddr *) &addr, sizeof(struct sockaddr_in));
    d->sfd_status[sfd_num] = 1;
    return result;
  } else {
    result = connect(new_sfd, (struct sockaddr *) &addr, sizeof(struct sockaddr_in));
  }
  return result;
}


void* run_fuse_thread(void* thread_info_data) {
  struct thread_info* ti = (struct thread_info*)thread_info_data;
  connect_to_server(ti->server, ti->port, ti->d, ti->sfd_num, 0, 0);
  ti->d->num_of_working_servers++;
  sem_post(&sem);
}

void get_time(char buffer[26]) {
  time_t log_time;
  struct tm* tm_info;
  time(&log_time);
  tm_info = localtime(&log_time);
  strftime(buffer, 26, "%Y-%m-%d %H:%M:%S", tm_info);
}

void control_server(char* server, char* port, struct data* d, int sfd_num) {
  FILE* fptr = d->errorlog_fptr;
  char buffer[26];
  while(1) {
    usleep(1000);
    struct protocol p;
    p.function_number = INFINITE;
    int break_status = 0;
    time_t start_time;
    time(&start_time);
    int result = DISCONNECT;
    pthread_mutex_lock(&lock);
    send(d->sfd[sfd_num], &p, sizeof(struct protocol), 0);
    recv(d->sfd[sfd_num], &result, sizeof(int), 0);
    if(result == OK_STATUS) {
      // OK
      pthread_mutex_unlock(&lock);
    } else {
      d->sfd_status[sfd_num] = -1;
      close(d->sfd[sfd_num]);
      d->num_of_working_servers--;
      time(&start_time);
      get_time(buffer); 
      fprintf(fptr, "\n%s%s%s%s%s", buffer, ": ", d->disk_name, ": disconnected from server: ", d->server_names[sfd_num]);
      pthread_mutex_unlock(&lock);
      while(1) {
        usleep(1000);
        time_t end_time;
        double seconds;
        time(&end_time);
        seconds = difftime(end_time, start_time);
        if(seconds >= d->timeout) {
          get_time(buffer);
          fprintf(fptr, "\n%s%s%s%s%f%s%s%s", buffer, ": ", d->disk_name, ": ",
                  seconds, " seconds waited, can not reconnect to ",
                  d->server_names[sfd_num], ". Need to connect to a new server");
          get_time(buffer);
          fprintf(fptr, "\n%s%s%s%s%s", buffer, ": ", d->disk_name,
                  ": closing server file descriptor, server name: ", d->server_names[sfd_num]);
          close(d->sfd[sfd_num]);
          d->num_of_healthy_servers--;
          get_time(buffer);
          if(d->num_of_healthy_servers == 0) {
            fprintf(fptr, "\n%s%s%s%s", buffer, ": ", d->disk_name, 
                      ": There are no healthy servers. shutting down the program...");
            break_status = 1;
          } else if(d->num_of_healthy_servers == 1) {
            fprintf(fptr, "\n%s%s%s%s", buffer, ": ", d->disk_name, ": There is only one healthy server");
            break_status = 1;
          } else if(d->num_of_healthy_servers == 2) {
            fprintf(fptr, "\n%s%s%s%s", buffer, ": ", d->disk_name, ": Connecting to hotswap server...");
            server[0] = '\0';
            port[0] = '\0'; 
            parseServerInfo(d->hotswap, server, port);
            int nsfd = socket(AF_INET, SOCK_STREAM, 0);
            int connection_status = connect_to_server(server, port, d, 0, nsfd, 1);
            if(connection_status == 0){
              d->sfd[2] = nsfd;
              d->num_of_working_servers++;
              d->sfd_status[2] = 1;
              sfd_num = 2;
              get_time(buffer); 
              fprintf(fptr, "\n%s%s%s%s", buffer, ": ", d->disk_name, ": Connected to hotswap server");
              d->hotswap_status = 1;
              int hsn = get_worker_sfd_num(d);
              get_time(buffer);
              copy_data(2, hsn, d);
              sem_wait(&sem1);
              get_time(buffer);  
              fprintf(fptr, "\n%s%s%s%s%d", buffer, ": ", d->disk_name,
                          ": number of healthy servers: ", d->num_of_healthy_servers); 
              fprintf(fptr, "\n%s%s%s%s%d", buffer, ": ", d->disk_name,
                                    ": number of working servers: ", d->num_of_working_servers);
              fprintf(fptr, "\n%s%s%s%s", buffer, ": ", d->disk_name,
                                    ": All data is copied to hotswap server");
            } else {
              d->sfd_status[2] = -1;
              d->num_of_healthy_servers--;
              break_status = 1;
              get_time(buffer);
              fprintf(fptr, "\n%s%s%s%s", buffer, ": ", d->disk_name, ": Can not conect to hotswap server");
              fprintf(fptr, "\n%s%s%s%s%d", buffer, ": ", d->disk_name,
                              ": number of healthy servers: ", d->num_of_healthy_servers);
              fprintf(fptr, "\n%s%s%s%s%d", buffer, ": ", d->disk_name,
                              ": number of working servers: ", d->num_of_working_servers);
            }
          }
          break;
        }
          int new_sfd = socket(AF_INET, SOCK_STREAM, 0);
          int connection_status = connect_to_server(server, port, d, sfd_num, new_sfd, 1);
          if(connection_status == 0){
          get_time(buffer);
          fprintf(fptr, "\n%s%s%s%s%s%s%f%s", buffer, ": ", d->disk_name, ": Reconnected to ", d->server_names[sfd_num],
                      " after ", seconds, " seconds. connection is renewd");
          pthread_mutex_lock(&lock);
          send(new_sfd, &p, sizeof(struct protocol), 0);
          recv(new_sfd, &result, sizeof(int), 0);
          if(result == OK_STATUS) {
            get_time(buffer);
            fprintf(fptr, "\n%s%s%s%s%s", buffer, ": ", d->disk_name, 
                          ": Status is OK, server name: ", d->server_names[sfd_num]);
            d->sfd_status[sfd_num] = 1;
            d->sfd[sfd_num] = new_sfd;
            d->num_of_working_servers++;
            pthread_mutex_unlock(&lock);
            break;
          } else {
            fprintf(fptr, "\n%s%s%s%s%s", buffer, ": ", d->disk_name, 
                          ": Status is DISCONNECT again, server name: ", d->server_names[sfd_num]);
            pthread_mutex_unlock(&lock);
          }
        }
        close(new_sfd);
      }
    }
    if(break_status == 1) {
      break;
    }
  }
  //fclose(fptr);
  close(d->sfd[sfd_num]);
}


void* run_infinite_thread(void* thread_info_data) {
  struct thread_info* ti = (struct thread_info*)thread_info_data;
  connect_to_server(ti->server, ti->port, ti->d, ti->sfd_num, 0, 0);
  sem_post(&sem);
  control_server(ti->server, ti->port, ti->d, ti->sfd_num);
}

void runClient(char* server, char* port, struct data* d, int sfd_num) {
  struct thread_info* ti1 = malloc(sizeof(struct thread_info));
  strcpy(ti1->server, server);
  strcpy(ti1->port, port);
  ti1->d = d;
  ti1->sfd_num = sfd_num;
  ti1->timeout = d->timeout;
  struct thread_info* ti2 = malloc(sizeof(struct thread_info));
  strcpy(ti2->server, server);
  strcpy(ti2->port, port);
  ti2->d = d;
  ti2->sfd_num = sfd_num;
  ti2->timeout = d->timeout;
  pthread_t thread_id1;
  pthread_t thread_id2;
  pthread_create(&thread_id1, NULL, run_infinite_thread, ti1);
  pthread_create(&thread_id2, NULL, run_fuse_thread, ti2);
  sem_wait(&sem);
  sem_wait(&sem);
}


void parseServerInfo(char* info, char server[1024], char port[1024]) {
  int i = 0;
  while(info[i] != ':') {
    strncat(server, info + i, 1);
    i++;
  }
  i++;
  while(info[i] != '\0') {
    strncat(port, info + i, 1);
    i++;
  }
}


int main(int argc, char *argv[]) {
  // ./client [-f] [-s] configFile
  assert(argc == 3 || argc == 2 || argc == 4);
  struct cfg * config = NULL; 
  if(argc == 4) {
    config = parse(argv[3]);
  } else if(argc == 3) {
    config = parse(argv[2]);
  } else if(argc == 2) {
    config = parse(argv[1]);
  }
  FILE* fptr = fopen(config->errorlog, "w");
  fprintf(fptr, "%s\n", "Errolog");
  fclose(fptr);
  fptr = fopen(config->errorlog, "a");
  int i = 0;
  int status;
  int wpid;
  for(i = 0; i < config->numStorages; i++) {
    pthread_mutex_init(&lock, NULL);
    sem_init(&sem, 0, 0);
    sem_init(&sem1, 0, 0);
    int pid;
    if((pid = fork()) == 0) {
      ci = malloc(sizeof(struct cache_info));
      ci->num_elems = 0;
      ci->capacity = 1;
      ci->cur_size = 0;
      int cnt = 1;
      char* cs = config->cacheSize;
      size_t len = strlen(cs);
      if(cs[len - 1] == 'K') {
        cnt *= 1024;
        cs[len - 1] = '\0';
      }
      if(cs[len - 1] == 'M') {
        cnt = 1024 * 1024;
        cs[len - 1] = '\0';
      }
      if(cs[len - 1] == 'G') {
        cnt = 1024 * 1024 * 1024;
        cs[len - 1] = '\0';
      }
      ci->max_size = cnt * atoi(cs);
      co = malloc(ci->capacity * sizeof(struct cache_object));

      if(argc == 4) {
        strcpy(argv[3], config->storages[i].mountpoint);
      } else if(argc == 3) {
        strcpy(argv[2], config->storages[i].mountpoint);
      } else if(argc == 2) {
        strcpy(argv[1], config->storages[i].mountpoint);
      } 

      struct data* d = malloc(sizeof(struct data));    
      d->disk_name[0] = '\0';
      strcpy(d->disk_name, config->storages[i].diskName);
      d->errorlog_fptr = fptr;
      strcpy(d->server_names[0], config->storages[i].servers[0]);
      strcpy(d->server_names[1], config->storages[i].servers[1]);
      strcpy(d->server_names[2], config->storages[i].hotswap);
      strcpy(d->hotswap, config->storages[i].hotswap);
      d->sfd_status[0] = -1;
      d->sfd_status[1] = -1;
      d->sfd_status[2] = -1;
      d->num_of_working_servers = 0;
      d->num_of_healthy_servers = 3;
      d->timeout = config->timeout;
      d->hotswap_status = 0;
      int j;
      for(j = 0; j < 2; j++) {
        char server[1024];
        char port[1024];
        server[0] = '\0';
        port[0] = '\0'; 
        parseServerInfo(config->storages[i].servers[j], server, port);
        d->sfd[j] = socket(AF_INET, SOCK_STREAM, 0); 
        runClient(server, port, d, j);
      }
      return fuse_main(argc, argv, &netRaid_oper, d);
    }
  }
  while((wpid = wait(&status)) > 0);
}