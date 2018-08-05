#pragma once

#include <stdint.h>

typedef enum {
  INSERT,
  SELECT
} statement_type;

typedef enum {
  INTERNAL,
  LEAF
} node_type;

typedef struct {
  uint32_t id;
  char username[33];
  char email[256];
} row;

#define TABLE_MAX_PAGES 100
#define PAGE_SIZE 4096

typedef struct {
  int fd;
  uint32_t flen;
  uint32_t npages;
  uint8_t* pages[TABLE_MAX_PAGES];
} pager;

typedef struct {
  pager* pager;
  uint32_t root_page_num;
} table;

typedef struct {
  statement_type type;
  row row;
} statement;

typedef struct {
  table* table;
  uint32_t pagen;
  uint32_t celln;
  short end_of_table;
} cursor;

uint8_t* cursor_value(cursor*);
table* db_open(const char*);
void db_close(table*);
cursor* table_start(table*);
cursor* table_find(table*, uint32_t);
void cursor_advance(cursor*);
uint8_t* get_page(pager*, uint32_t);

extern const uint32_t NODE_T_SIZE;
extern const uint32_t NODE_T_OFFSET;
extern const uint32_t IS_ROOT_SIZE;
extern const uint32_t IS_ROOT_OFFSET;
extern const uint32_t PARENT_PTR_SIZE;
extern const uint32_t PARENT_PTR_OFFSET;
extern const uint8_t NODE_HDR_SIZE;

extern const uint32_t LNODE_NUM_CELLS_SIZE;
extern const uint32_t LNODE_NUM_CELLS_OFFSET;
extern const uint32_t LNODE_HDR_SIZE;

extern const uint32_t LNODE_KEY_SIZE;
extern const uint32_t LNODE_KEY_OFFSET;
extern const uint32_t LNODE_VALUE_SIZE;
extern const uint32_t LNODE_VALUE_OFFSET;
extern const uint32_t LNODE_CELL_SIZE;
extern const uint32_t LNODE_SPACE_FOR_CELLS;
extern const uint32_t LNODE_MAX_CELLS;

uint32_t* lnode_next_leaf(uint8_t*);
uint32_t* lnode_num_cells(uint8_t*);
uint8_t* lnode_cell(uint8_t*, uint32_t);
uint32_t* lnode_key(uint8_t*, uint32_t);
uint8_t* lnode_value(uint8_t*, uint32_t);
void initialize_lnode(uint8_t*);
void lnode_insert(cursor*, uint32_t, row*);
cursor* lnode_find(table*, uint32_t, uint32_t);
node_type get_node_type(uint8_t*);
void set_node_type(uint8_t*, node_type);

uint32_t* inode_num_keys(uint8_t*);
uint32_t* inode_key(uint8_t*, uint32_t);
uint32_t* inode_child(uint8_t* node, uint32_t child_num);
uint32_t* inode_right_child(uint8_t* node);
cursor* inode_find(table*, uint32_t, uint32_t);
void inode_insert(table*, uint32_t, uint32_t);
