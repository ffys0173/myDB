#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#pragma warning ( disable : 4996 )

#define LEAF_ORDER 31
#define INTERNAL_ORDER 248
#define PAGE_SIZE 4096
#define TRUE 1
#define FALSE 0

typedef uint64_t pagenum_t;

typedef struct {
	uint64_t free_page_number;
	uint64_t root_page_number;
	uint64_t number_of_pages;
	char reserved[4072];
}header;

typedef struct {
	uint64_t next_page;
	char reserved[4088];
}free_page;

typedef struct {
	uint64_t key;
	char value[120];
}leaf_page;

typedef struct {
	uint64_t key;
	uint64_t page_number;
}internal_page;

typedef union {
	leaf_page leaf[LEAF_ORDER];
	internal_page internal[INTERNAL_ORDER];
}datatype;

typedef struct {
	int is_leaf;
	int num_of_keys;
	uint64_t parent_page_number;
	uint64_t sibling_pn;
	char reserved[102];
	datatype data;
}node;

typedef union {
	header header;
	node node;
	free_page free;
}page_t;

//file api
pagenum_t file_alloc_page();
void file_free_page(pagenum_t pagenum);
void file_read_page(pagenum_t pagenum, page_t* dest);
void file_write_page(pagenum_t pagenum, const page_t* src);

//operations api
int open_table(char* pathname);
int db_insert(uint64_t key, char* value);
int db_find(uint64_t key, char* ret_val);
int db_delete(uint64_t key);

pagenum_t find_node(uint64_t key);
void init_bpt();
int leaf_split(page_t* old, pagenum_t old_num);
int insert_into_internal(pagenum_t cur_num, uint64_t key, pagenum_t value);
void sort(page_t* page);

FILE* fp;
int fd;

page_t header_page;