COMPILER = gcc -g
FILESYSTEM_FILES_CLIENT = parser.c net_raid_client.c
FILESYSTEM_FILES_SERVER = net_raid_server.c

build: $(FILESYSTEM_FILES)
	$(COMPILER) $(FILESYSTEM_FILES_CLIENT) -o net_raid_client `pkg-config fuse --cflags --libs` -lpthread 
	$(COMPILER) $(FILESYSTEM_FILES_SERVER) -o net_raid_server -lssl -lcrypto
	@echo './net_raid_client -f -s configFile'
	@echo './net_raid_server'