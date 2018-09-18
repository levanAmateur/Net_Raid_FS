#include "parser.h"

const int MAXIMUM_LINE_LENGTH = 256;

void read1(struct cfg* c, char* line) {
	char* var = NULL;
	char* val = NULL;
	int cnt = 0;
	char* parsedLine = strtok(line, " =\n");
	char* errorlog = strdup("errorlog");
	char* cacheSize = strdup("cache_size");
	char* cacheReplacment = strdup("cache_replacment");
	char* timeout = strdup("timeout");
	char* end = strdup("\0");

	while(parsedLine != NULL) {
		if(cnt == 0) {
			var = strdup(parsedLine);
			strcat(var, end);
			cnt++;
		} else {
			int size = strlen(parsedLine);
			val = strdup(parsedLine);
			strcat(val, end);	
			memcpy(val, parsedLine, size);
			if(strcmp(var, errorlog) == 0) {
				c->errorlog = strdup(val);
			}
			if(strcmp(var, cacheSize) == 0) {
				c->cacheSize = strdup(val);
			}
			if(strcmp(var, cacheReplacment) == 0) {
				c->cacheReplacment = strdup(val);
			}
			if(strcmp(var, timeout) == 0) {
				int timeout = atoi(val);
				c->timeout = timeout;
			}
			cnt--;
			free(var);
			free(val);
		}
		parsedLine = strtok(NULL, " =\n");
	}
	free(errorlog);
	free(cacheSize);
	free(cacheReplacment);
	free(timeout);
	free(end);
	free(parsedLine);
}

int read = 1;

void read2(struct cfg* c, char* line) {
	char* var = NULL;
	char* val = NULL;
	char* servers = strdup("servers");
	char* diskName = strdup("diskname");
	char* mountpoint = strdup("mountpoint");
	char* raid = strdup("raid");
	char* hotswap = strdup("hotswap");
	char* end = strdup("\0");
	int cnt = 0;
	if(read == 1) {
		c->numStorages++;
		if(c->numStorages == 1) {
			c->storages = malloc(sizeof(struct storage));
		} else {
			c->storages = realloc(c->storages, c->numStorages * sizeof(struct storage));
		}
		struct storage* tmp = malloc(sizeof(struct storage));
		memcpy((char*)c->storages + sizeof(struct storage) * (c->numStorages - 1), tmp, sizeof(struct storage));
		free(tmp);
		read = 0;
	} else {
		char* parsedLine = strtok(line, " =\n");
		while(parsedLine != NULL) {
			if(cnt == 0) {
				var = strdup(parsedLine);
				strcat(var, end);
				cnt++;
				if(strcmp(var, servers) == 0) {
					parsedLine = strtok(NULL, " =\n,");
					c->storages[c->numStorages - 1].numServers = 0;
					c->storages[c->numStorages - 1].servers = malloc(0);
					while(parsedLine != NULL) {
						c->storages[c->numStorages - 1].numServers++;
						int numServers = c->storages[c->numStorages - 1].numServers;
						c->storages[c->numStorages - 1].servers = realloc(c->storages[c->numStorages - 1].servers,
							numServers * sizeof(char*));
						c->storages[c->numStorages - 1].servers[numServers - 1] = strdup(parsedLine);
						parsedLine = strtok(NULL, " =\n,");
					}
					free(parsedLine);
					free(var);
				}
			} else {
				val = strdup(parsedLine);
				strcat(val, end);
				if(strcmp(var, diskName) == 0) {
					c->storages[c->numStorages - 1].diskName = strdup(val);
				}
				if(strcmp(var, mountpoint) == 0) {
					c->storages[c->numStorages - 1].mountpoint = strdup(val);
				}
				if(strcmp(var, raid) == 0) {
					c->storages[c->numStorages - 1].raid = strdup(val);		
				}
				if(strcmp(var, hotswap) == 0) {
					c->storages[c->numStorages - 1].hotswap = strdup(val);
					read = 1;
				}
				cnt--;
				free(var);
				free(val);
			}
			parsedLine = strtok(NULL, " =\n");
		}
		free(parsedLine);
	}
	free(servers);
	free(diskName);
	free(mountpoint);
	free(raid);
	free(hotswap);
	free(end);
}

void putInStruct(struct cfg* c, char* line, int lineNum) {
	if(lineNum < 4) {
		read1(c, line);
	} else {
		read2(c, line);
	}
}

struct cfg* parse(char* fileName) {
	FILE* file = NULL;
	file = fopen(fileName, "r");
	assert(file != NULL);
	struct cfg* result = malloc(sizeof(struct cfg));
	result->numStorages = 0;
	char line[MAXIMUM_LINE_LENGTH];
	int lineNum = 0;
	while(fgets(line, MAXIMUM_LINE_LENGTH, file) != NULL) {    
	    putInStruct(result, line, lineNum);
		lineNum++;	
	}
	fclose(file);
	return result;
}


void destroyStorage(struct storage* st) {
	free(st->diskName);
	free(st->mountpoint);
	free(st->raid);
	free(st->hotswap);
	int i = 0;
	for(i = 0; i < st->numServers; i++) {
		free(st->servers[i]);
	}
	free(st->servers);
}

void destroy(struct cfg* config) {
	free(config->errorlog);
	free(config->cacheSize);
	free(config->cacheReplacment);
	int i = 0;
	for(i = 0; i < config->numStorages; i++) {
		destroyStorage(&config->storages[i]);
	}
	free(config->storages);
	free(config);
}