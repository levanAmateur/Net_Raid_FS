#ifndef PARSER_H
#define PARSER_H

#include <string.h>
#include <stdio.h>
#include "assert.h"
#include <malloc.h>
#include <stdlib.h>

struct storage {
	char* diskName;
	char* mountpoint;
	char* raid;
	char** servers;
	char* hotswap;
	int numServers;
};

struct cfg {
	char* errorlog;
	char* cacheSize;
	char* cacheReplacment;
	int timeout;
	struct storage* storages;
	int numStorages;
};

typedef char fixed_size_string[256];

struct data {
	char disk_name[256];
	int sfd[3];
	int sfd_status[3];
	fixed_size_string server_names[3];
	FILE* errorlog_fptr;
	char hotswap[256];
	int num_of_healthy_servers;
	int num_of_working_servers;
	int hotswap_status;
	int timeout;
};


struct cfg* parse(char* file);

void destroy(struct cfg* config);

#endif