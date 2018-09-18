#ifndef NET_RAID_PROTOCOL_H
#define NET_RAID_PROTOCOL_H

struct protocol {
  int function_number;
  char path[PATH_MAX];
  char newpath[PATH_MAX];
  mode_t mode;
  off_t newsize;
  size_t size;
  off_t offset;
  int flags;
  int recover;
  int append;
};

struct getattr_protocol {
  int status;
  struct stat sb;
};

struct opendir_protocol {
  int status;
  DIR* dp;
};

struct open_protocol {
  int status;
  int fd;
};

typedef char fixed_size_path[PATH_MAX + 1];

/*
  Function names
*/
static const int GETATTR = 1;
static const int OPENDIR = 2;
static const int READDIR = 3;
static const int MKNOD = 4;
static const int MKDIR = 5;
static const int RENAME = 6;
static const int RMDIR = 7;
static const int UNLINK = 8;
static const int OPEN = 9;
static const int READ = 10;
static const int RELEASEDIR = 11;
static const int RELEASE = 12;
static const int TRUNCATE = 13;
static const int WRITE = 14;
static const int INFINITE = 15;
static const int FILE_PATH = 16;

static const int DISCONNECT = -1;
static const int OK_STATUS = 1;


#endif