#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <dirent.h>
#include <sys/stat.h>
#include <utime.h>
#include <sys/time.h>
#include "net_raid_protocol.h"
#include <openssl/md5.h>
#include <sys/xattr.h>
#include <fts.h>
#include <sys/epoll.h>

#define BACKLOG 10

static int MAXIMUM_NUMBER_OF_EVENTS = 1;
static char serverRoot[PATH_MAX];

void netRaid_concat_server_absoulte_path(char absolute[PATH_MAX], char* path);
int hash(char * filename, char* hash);
void serverSide_netRaid_getattr(struct protocol* p, int cfd, char absolute[PATH_MAX]);
void serverSide_netRaid_opendir(struct protocol* p, int cfd, char absolute[PATH_MAX]);
void serverSide_netRaid_readdir(struct protocol* p, int cfd, char absolute[PATH_MAX]);
void serverSide_netRaid_mknod(struct protocol* p, int cfd, char absolute[PATH_MAX]);
void serverSide_netRaid_mkdir(struct protocol* p, int cfd, char absolute[PATH_MAX]);
void serverSide_netRaid_rename(struct protocol* p, int cfd, char absolute[PATH_MAX], char new_absolute[PATH_MAX]);
void serverSide_netRaid_rmdir(struct protocol* p, int cfd, char absolute[PATH_MAX]);
void serverSide_netRaid_unlink(struct protocol* p, int cfd, char absolute[PATH_MAX]);
void serverSide_netRaid_open(struct protocol* p, int cfd, char absolute[PATH_MAX]);
void serverSide_netRaid_read(struct protocol* p, int cfd, char absolute[PATH_MAX]);
void serverSide_netRaid_releasedir(struct protocol* p, int cfd, char absolute[PATH_MAX]);
void serverSide_netRaid_release(struct protocol* p, int cfd, char absolute[PATH_MAX]);
void serverSide_netRaid_truncate(struct protocol* p, int cfd, char absolute[PATH_MAX]);
void serverSide_netRaid_write(struct protocol* p, int cfd, char absolute[PATH_MAX]);


void netRaid_concat_server_absoulte_path(char absolute[PATH_MAX], char* path) {
    strcpy(absolute, serverRoot);
    strncat(absolute, path, PATH_MAX); 
}


/*
  This function (int hash(char * filename, char* hash)) is copied from stackoverflow.com
*/
int hash(char * filename, char* hash) {
    int MD_DIGEST_LENGTH = 16;
    unsigned char c[MD_DIGEST_LENGTH];

    int i;
    FILE *inFile = fopen (filename, "rb");
    MD5_CTX mdContext;
    int bytes;
    unsigned char data[1024];

    if (inFile == NULL) {
        printf ("%s can't be opened.\n", filename);
        return 0;
    }

    MD5_Init (&mdContext);
    while ((bytes = fread (data, 1, 1024, inFile)) != 0)
        MD5_Update (&mdContext, data, bytes);

    MD5_Final (c,&mdContext);

    char md5string[33];
    for(i = 0; i < 16; ++i)
      sprintf(&md5string[i*2], "%02x", (unsigned int)c[i]);

    strcpy(hash, md5string);
    fclose (inFile);

    return 0;
}

//Stackoverflow is used to write this function
void get_file_path(struct protocol* p, int cfd) {
  FTS* fs = NULL;
  FTSENT* parent = NULL;
  FTSENT* child = NULL;
  int fts_options = FTS_COMFOLLOW | FTS_LOGICAL | FTS_NOCHDIR;
  int val = 0;
  char* ppp[] = {serverRoot, NULL};
  fs = fts_open(ppp, fts_options, NULL);
  int count = 0;
  while((parent = fts_read(fs)) != NULL) {
    count++;
  }
  int size = count;
  send(cfd, &size, sizeof(int), 0);
  //
  fs = NULL;
  parent = NULL;
  child = NULL;
  fixed_size_path fsp_list[count];
  val = 0;
  char* pppp[] = {serverRoot, NULL};
  fs = fts_open(pppp, fts_options, NULL);
  int i = 0;
  int md[count];
  while((parent = fts_read(fs)) != NULL) {
    fixed_size_path cur;
    size_t len = strlen(serverRoot);
    if(parent->fts_info == FTS_D) {
      strcpy(cur, strdup("d"));
    } else if(parent->fts_info == FTS_F) {
      strcpy(cur, strdup("f"));
    }
    struct stat sb;
    lstat(parent->fts_path, &sb);
    md[i] = sb.st_mode;
    strcpy(cur + 1, parent->fts_path + len);
    strcpy(fsp_list[i], cur);
    i++;
  }
  for(i = 0; i < count; i++) {
    send(cfd, fsp_list[i], PATH_MAX + 1, 0);
    send(cfd, &md[i], sizeof(int), 0);
  }
}


void serverSide_netRaid_getattr(struct protocol* p, int cfd, char absolute[PATH_MAX]) {
  struct stat sb;
  int result = lstat(absolute, &sb);
  if(result < 0) {
     result = -errno;
  }
  struct getattr_protocol gp;
  gp.status = result;
  memcpy(&(gp.sb), &sb, sizeof(struct stat));
  send(cfd, &gp, sizeof(struct getattr_protocol), 0);
}


void serverSide_netRaid_opendir(struct protocol* p, int cfd, char absolute[PATH_MAX]) {
  int result = 0;
  DIR *dp;
  struct opendir_protocol op;
  dp = opendir(absolute);
  if(dp == NULL) {
    result = -errno;
  }
  op.dp = dp;
  op.status = result;
  send(cfd, &op, sizeof(struct opendir_protocol), 0);
}


void serverSide_netRaid_readdir(struct protocol* p, int cfd, char absolute[PATH_MAX]) {
  struct dirent* drnt;
  DIR* dp = opendir(absolute);
  drnt = readdir(dp);
  char str[4096];
  str[4095] = '\0';
  str[0] = '\0';
  int result = 0;
  if(drnt == 0) {
   result = -errno;
  } else {
    while(1) {
      if(drnt == NULL) break;
      strcat(str, drnt->d_name);
      strcat(str, "/");
      drnt = readdir(dp);
    }
  }
  strcat(str, "");
  size_t size = strlen(str) + 1;
  send(cfd, &size, sizeof(size_t), 0);
  send(cfd, str, size, 0);
  send(cfd, &result, sizeof(int), 0);
}


void serverSide_netRaid_mknod(struct protocol* p, int cfd, char absolute[PATH_MAX]) {
  int result;
  if(S_ISREG(p->mode)) {
    result = open(absolute, O_CREAT | O_EXCL | O_WRONLY, p->mode);
    if(result >= 0) {
      result = close(result);
    }
  }
  if(result < 0) {
    result = -errno;
  }
  char buf[33];
  hash(absolute, buf);
  setxattr(absolute, "user.hash", buf, 33, 0);
  send(cfd, &result, sizeof(int), 0);
}


void serverSide_netRaid_mkdir(struct protocol* p, int cfd, char absolute[PATH_MAX]) {
  int result = mkdir(absolute, p->mode);
  if(result < 0) {
    result = -errno;
  }
  send(cfd, &result, sizeof(int), 0);
}


void serverSide_netRaid_rename(struct protocol* p, int cfd, char absolute[PATH_MAX], char new_absolute[PATH_MAX]) {
  int result = rename(absolute, new_absolute);
  if(result < 0) {
    result = -errno;
  }
  char buf[33];
  hash(new_absolute, buf);
  setxattr(new_absolute, "user.hash", buf, 33, 0);
  send(cfd, &result, sizeof(int), 0);
}


void serverSide_netRaid_rmdir(struct protocol* p, int cfd, char absolute[PATH_MAX]) {
  int result = rmdir(absolute);
  if(result < 0) {
    result = -errno;
  }
  send(cfd, &result, sizeof(int), 0);
}


void serverSide_netRaid_unlink(struct protocol* p, int cfd, char absolute[PATH_MAX]) {
  int result = unlink(absolute);
  if(result < 0) {
    result = -errno;
  }
  send(cfd, &result, sizeof(int), 0);
}


void serverSide_netRaid_open(struct protocol* p, int cfd, char absolute[PATH_MAX]) {
  int result = 0;
  int fd = open(absolute, p->flags);
  if (fd < 0){
    fd = -errno;
    result = -errno;
  }
  struct open_protocol op;
  op.status = result;
  op.fd = fd;
  send(cfd, &op, sizeof(struct open_protocol), 0);
  char old_buf[33];
  getxattr(absolute, "user.hash", old_buf, 33);
  char new_buf[33];
  hash(absolute, new_buf);
  send(cfd, old_buf, 33, 0);
  send(cfd, new_buf, 33, 0);
}


void serverSide_netRaid_read(struct protocol* p, int cfd, char absolute[PATH_MAX]) {
  int fd = open(absolute, O_RDONLY);
  char buf[p->size + 1];
  int result = pread(fd, buf, p->size, p->offset);
  if(result < 0) {
    result = -errno;
  }
  buf[p->size] = '\0';
  send(cfd, &result, sizeof(int), 0);
  send(cfd, buf, p->size, 0);
}


void serverSide_netRaid_releasedir(struct protocol* p, int cfd, char absolute[PATH_MAX]) {
  int result = 0;
  DIR* dp = opendir(absolute);
  if(dp != NULL) {
    closedir(dp);
  }
  send(cfd, &result, sizeof(int), 0);
}


void serverSide_netRaid_release(struct protocol* p, int cfd, char absolute[PATH_MAX]) {
  int fd = open(absolute, O_RDONLY);
  int result = close(fd);
  if(result < 0) {
    result = -errno;
  }
  send(cfd, &result, sizeof(int), 0);  
}



void serverSide_netRaid_truncate(struct protocol* p, int cfd, char absolute[PATH_MAX]) {
  int result = truncate(absolute, p->newsize);
  if(result < 0) {
    result = -errno;
  }
  char buf[33];
  hash(absolute, buf);
  setxattr(absolute, "user.hash", buf, 33, 0);
  send(cfd, &result, sizeof(int), 0); 
}


void serverSide_netRaid_write(struct protocol* p, int cfd, char absolute[PATH_MAX]) {
  int result;
  if(p->recover == 1) { // File is damaged on one server...
    int fd = open(absolute, O_WRONLY);
    FILE* fptr = NULL;
    if(p->append == 0) {
      fptr = fopen(absolute, "w");
    } else {
      fptr = fopen(absolute, "a");
    }
    char buf[p->size + 1];
    buf[p->size] = '\0';
    recv(cfd, buf, p->size, 0);
    result = pwrite(fd, buf, p->size, p->offset);
    if(result < 0) {
      result = -errno;
    }
    fclose(fptr);
  } else { // aq ara
    char buf[p->size + 1];
    buf[p->size] = '\0';
    recv(cfd, buf, p->size, 0);
    int fd = open(absolute, O_WRONLY);
    result = pwrite(fd, buf, p->size, p->offset);
    if(result < 0) {
      result = -errno;
    }
    close(fd);
  }
  char hbuf[33];
  hash(absolute, hbuf);
  setxattr(absolute, "user.hash", hbuf, 33, 0);
  send(cfd, &result, sizeof(int), 0);
}



int main(int argc, char* argv[]){
  int sfd;
  struct sockaddr_in addr;
  struct sockaddr_in peer_addr;
  int port = atoi(argv[2]);
  serverRoot[0] = '\0';
  strcat(serverRoot, argv[3]);
  sfd = socket(AF_INET, SOCK_STREAM, 0);
  int optval = 1;
  setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(int));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  bind(sfd, (struct sockaddr *) &addr, sizeof(struct sockaddr_in));
  listen(sfd, BACKLOG);
  int peer_addr_size = sizeof(struct sockaddr_in);
  int cfd = accept(sfd, (struct sockaddr *) &peer_addr, &peer_addr_size);

  int efd = epoll_create(1);
  static struct epoll_event ev;
  struct epoll_event* events;
  ev.events = EPOLLIN | EPOLLPRI | EPOLLERR | EPOLLHUP;
  ev.data.fd = cfd;
  int res = epoll_ctl(efd, EPOLL_CTL_ADD, cfd, &ev);
  events = calloc (MAXIMUM_NUMBER_OF_EVENTS, sizeof ev);
  while(1) {
    int nfds = epoll_wait(efd, events, 1, -1);
    int i;
    for(i = 0; i < nfds; i++) {
      cfd = events[i].data.fd;
      struct protocol p;
      recv(cfd, &p, sizeof(struct protocol), 0);
      if(p.function_number == FILE_PATH) {
        get_file_path(&p, cfd);
      } else if(p.function_number == INFINITE) {
        int res_status = OK_STATUS;
        send(cfd, &res_status, sizeof(int), 0);
      } else if(p.function_number == GETATTR) {
        char absolute[PATH_MAX];
        netRaid_concat_server_absoulte_path(absolute, p.path);
        serverSide_netRaid_getattr(&p, cfd, absolute);
      } else if(p.function_number == OPENDIR) {
        char absolute[PATH_MAX];
        netRaid_concat_server_absoulte_path(absolute, p.path);
        serverSide_netRaid_opendir(&p, cfd, absolute);
      } else if(p.function_number == READDIR) {
        char absolute[PATH_MAX];
        netRaid_concat_server_absoulte_path(absolute, p.path);
        serverSide_netRaid_readdir(&p, cfd, absolute);
      } else if(p.function_number == MKNOD) {
        char absolute[PATH_MAX];
        netRaid_concat_server_absoulte_path(absolute, p.path);
        serverSide_netRaid_mknod(&p, cfd, absolute);
      } else if(p.function_number == MKDIR) {
        char absolute[PATH_MAX];
        netRaid_concat_server_absoulte_path(absolute, p.path);
        serverSide_netRaid_mkdir(&p, cfd, absolute);
      } else if(p.function_number == RENAME) {
        char absolute[PATH_MAX];
        netRaid_concat_server_absoulte_path(absolute, p.path);
        char new_absolute[PATH_MAX];
        netRaid_concat_server_absoulte_path(new_absolute, p.newpath);
        serverSide_netRaid_rename(&p, cfd, absolute, new_absolute);
      } else if(p.function_number == RMDIR) {
        char absolute[PATH_MAX];
        netRaid_concat_server_absoulte_path(absolute, p.path);
        serverSide_netRaid_rmdir(&p, cfd, absolute);
      } else if(p.function_number == UNLINK) {
        char absolute[PATH_MAX];
        netRaid_concat_server_absoulte_path(absolute, p.path);
        serverSide_netRaid_unlink(&p, cfd, absolute);
      } else if(p.function_number == OPEN) {
        char absolute[PATH_MAX];
        netRaid_concat_server_absoulte_path(absolute, p.path);
        serverSide_netRaid_open(&p, cfd, absolute);
      } else if(p.function_number == READ) {
        char absolute[PATH_MAX];
        netRaid_concat_server_absoulte_path(absolute, p.path);
        serverSide_netRaid_read(&p, cfd, absolute);
      } else if(p.function_number == RELEASEDIR) {
        char absolute[PATH_MAX];
        netRaid_concat_server_absoulte_path(absolute, p.path);
        serverSide_netRaid_releasedir(&p, cfd, absolute);
      } else if(p.function_number == RELEASE) {
        char absolute[PATH_MAX];
        netRaid_concat_server_absoulte_path(absolute, p.path);
        serverSide_netRaid_release(&p, cfd, absolute);
      } else if(p.function_number == TRUNCATE) {
        char absolute[PATH_MAX];
        netRaid_concat_server_absoulte_path(absolute, p.path);
        serverSide_netRaid_truncate(&p, cfd, absolute);
      } else if(p.function_number == WRITE) {
        char absolute[PATH_MAX];
        netRaid_concat_server_absoulte_path(absolute, p.path);
        serverSide_netRaid_write(&p, cfd, absolute);
      }
    }
  }
  close(sfd);
}