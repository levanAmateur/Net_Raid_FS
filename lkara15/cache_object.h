#ifndef CACHE_OBJECT_H
#define CACHE_OBJECT_H

struct cache_object {
  char name[PATH_MAX]; // path
  char* content;
  off_t offset;
  size_t size;
};

struct cache_info {
  int num_elems;
  int capacity;
  size_t cur_size; // bytes
  size_t max_size; // bytes
};

#endif