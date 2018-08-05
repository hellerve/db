#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "data.h"
#include "serialize.h"

const uint32_t NODE_T_SIZE = sizeof(uint8_t);
const uint32_t NODE_T_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = NODE_T_SIZE;
const uint32_t PARENT_PTR_SIZE = sizeof(uint32_t);
const uint32_t PARENT_PTR_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint8_t NODE_HDR_SIZE = NODE_T_SIZE + IS_ROOT_SIZE + PARENT_PTR_SIZE;

const uint32_t LNODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LNODE_NUM_CELLS_OFFSET = NODE_HDR_SIZE;
const uint32_t LNODE_HEADER_SIZE = NODE_HDR_SIZE + LNODE_NUM_CELLS_SIZE;

const uint32_t LNODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LNODE_KEY_OFFSET = 0;
const uint32_t LNODE_VALUE_SIZE = sizeof(row);
const uint32_t LNODE_VALUE_OFFSET = LNODE_KEY_OFFSET + LNODE_KEY_SIZE;
const uint32_t LNODE_CELL_SIZE = LNODE_KEY_SIZE + LNODE_VALUE_SIZE;
const uint32_t LNODE_SPACE_FOR_CELLS = PAGE_SIZE - LNODE_HEADER_SIZE;
const uint32_t LNODE_MAX_CELLS = LNODE_SPACE_FOR_CELLS / LNODE_CELL_SIZE;

const uint32_t LNODE_RIGHT_SPLIT_COUNT = (LNODE_MAX_CELLS+1) / 2;
const uint32_t LNODE_LEFT_SPLIT_COUNT =
  (LNODE_MAX_CELLS+1) - LNODE_RIGHT_SPLIT_COUNT;

const uint32_t INODE_NUM_KEYS_SIZE = sizeof(uint32_t);
const uint32_t INODE_NUM_KEYS_OFFSET = NODE_HDR_SIZE;
const uint32_t INODE_RIGHT_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INODE_RIGHT_CHILD_OFFSET =
  INODE_NUM_KEYS_OFFSET + INODE_NUM_KEYS_SIZE;
const uint32_t INODE_HDR_SIZE =
  NODE_HDR_SIZE + INODE_NUM_KEYS_SIZE + INODE_RIGHT_CHILD_SIZE;

const uint32_t INODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t INODE_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INODE_CELL_SIZE = INODE_CHILD_SIZE + INODE_KEY_SIZE;

unsigned char is_node_root(unsigned char* node) {
  return *(node + IS_ROOT_OFFSET);
}

void set_node_root(unsigned char* node, unsigned char is_root) {
  *(node + IS_ROOT_OFFSET) = is_root;
}

unsigned char* get_page(pager* p, uint32_t page_num) {
  if (page_num >= TABLE_MAX_PAGES) {
    printf("Tried to fetch page number out of bounds: %u > %d.\n", page_num,
           TABLE_MAX_PAGES);
    exit(1);
  }

  if (p->pages[page_num] == NULL) {
    unsigned char* page = malloc(PAGE_SIZE);
    uint32_t num_pages = p->flen / PAGE_SIZE;

    if (p->flen % PAGE_SIZE) num_pages += 1;

    if (page_num <= num_pages) {
      lseek(p->fd, page_num * PAGE_SIZE, SEEK_SET);
      ssize_t bytes_read = read(p->fd, page, PAGE_SIZE);
      if (bytes_read == -1) {
        printf("Error reading file: %d\n", errno);
        exit(1);
      }
    }

    p->pages[page_num] = page;

    if (page_num >= p->npages) {
      p->npages = page_num + 1;
    }
  }
  return p->pages[page_num];
}

uint32_t get_unused_page_num(pager* p) {
  return p->npages;
}

unsigned char* cursor_value(cursor* c) {
  uint32_t page_num = c->pagen;
  unsigned char* page = get_page(c->table->pager, page_num);
  return lnode_value(page, c->celln);
}

void cursor_advance(cursor* c) {
  uint32_t page_num = c->pagen;
  unsigned char* node = get_page(c->table->pager, page_num);

  c->celln += 1;

  if (c->celln >= *lnode_num_cells(node)) c->end_of_table = 1;
}

pager* pager_open(const char* filename) {
  int i;
  off_t flen;
  pager* p;
  int fd = open(filename, O_RDWR|O_CREAT, S_IWUSR|S_IRUSR);

  if (fd < 0) {
    puts("Error opening DB file.");
    exit(1);
  }

  flen = lseek(fd, 0, SEEK_END);

  p = malloc(sizeof(pager));
  p->fd = fd;
  p->flen = flen;
  p->npages = flen / PAGE_SIZE;

  if (flen % PAGE_SIZE) {
    puts("DB file is not a whole number of pages. Corrupt file.");
    exit(1);
  }

  for (i = 0; i < TABLE_MAX_PAGES; i++) p->pages[i] = NULL;

  return p;
}

table* db_open(const char* filename) {
  unsigned char* root;
  pager* p = pager_open(filename);

  table* res = malloc(sizeof(table));
  res->pager = p;
  res->root_page_num = 0;

  if (!p->npages) {
    root = get_page(p, 0);
    initialize_lnode(root);
    set_node_root(root, 1);
  }

  return res;
}

void pager_flush(pager* p, uint32_t page_num) {
  if (!p->pages[page_num]) {
    puts("Tried to flush null page.");
    exit(1);
  }

  if (lseek(p->fd, page_num*PAGE_SIZE, SEEK_SET) < 0) {
    printf("Error seeking: %d.\n", errno);
    exit(1);
  }

  if (write(p->fd, p->pages[page_num], PAGE_SIZE) < 0) {
    printf("Error writing: %d.\n", errno);
    exit(1);
  }
}

void db_close(table* t) {
  int i;
  unsigned char* page;
  pager* p = t->pager;

  for (i = 0; i < p->npages; i++) {
    if (!p->pages[i]) continue;

    pager_flush(p, i);
  }

  if (close(p->fd) < 0) {
    puts("Error closing DB file.");
    exit(1);
  }

  for (i = 0; i < TABLE_MAX_PAGES; i++) {
    page = p->pages[i];

    if (page) free(page);
  }

  free(p);
}

cursor* table_start(table* t) {
  int ncells;
  cursor* c = malloc(sizeof(cursor));
  c->table = t;
  c->pagen = t->root_page_num;
  c->celln = 0;

  ncells = *lnode_num_cells(get_page(t->pager, t->root_page_num));
  c->end_of_table = ncells == 0;

  return c;
}

cursor* table_find(table* t, uint32_t key) {
  uint32_t root_page_num = t->root_page_num;
  unsigned char* root = get_page(t->pager, root_page_num);

  if (get_node_type(root) == LEAF) {
    return lnode_find(t, root_page_num, key);
  } else {
    exit(1);
  }
}

uint32_t* inode_num_keys(unsigned char* node) {
  return (uint32_t*) (node + INODE_NUM_KEYS_OFFSET);
}

void initialize_inode(unsigned char* node) {
  set_node_type(node, INTERNAL);
  set_node_root(node, 0);
  *inode_num_keys(node) = 0;
}

uint32_t* inode_right_child(unsigned char* node) {
  return (uint32_t*)(node + INODE_RIGHT_CHILD_OFFSET);
}

uint32_t* inode_cell(unsigned char* node, uint32_t cell_num) {
  return (uint32_t*)(node + INODE_HDR_SIZE + cell_num * INODE_CELL_SIZE);
}

uint32_t* inode_child(unsigned char* node, uint32_t child_num) {
  uint32_t num_keys = *inode_num_keys(node);
  if (child_num > num_keys) {
    printf("Tried to access child_num %d > num_keys %d\n", child_num, num_keys);
    exit(1);
  } else if (child_num == num_keys) {
    return inode_right_child(node);
  } else {
    return inode_cell(node, child_num);
  }
}

uint32_t* inode_key(unsigned char* node, uint32_t key_num) {
  return inode_cell(node, key_num) + INODE_CHILD_SIZE;
}

uint32_t* lnode_num_cells(unsigned char* node) {
  return (uint32_t*) (node + LNODE_NUM_CELLS_OFFSET);
}

unsigned char* lnode_cell(unsigned char* node, uint32_t cell_num) {
  return node + LNODE_HEADER_SIZE + cell_num * LNODE_CELL_SIZE;
}

uint32_t* lnode_key(unsigned char* node, uint32_t cell_num) {
  return (uint32_t*) lnode_cell(node, cell_num);
}

unsigned char* lnode_value(unsigned char* node, uint32_t cell_num) {
  return lnode_cell(node, cell_num) + LNODE_KEY_SIZE;
}

void initialize_lnode(unsigned char* node) {
  *lnode_num_cells(node) = 0;
  set_node_type(node, LEAF);
  set_node_root(node, 0);
}

uint32_t get_node_max_key(unsigned char* node) {
  switch (get_node_type(node)) {
    case INTERNAL:
      return *inode_key(node, *inode_num_keys(node) - 1);
    case LEAF:
      return *lnode_key(node, *lnode_num_cells(node) - 1);
  }
}


void create_new_root(table* t, uint32_t right_pn) {
  unsigned char* root = get_page(t->pager, t->root_page_num);
  //unsigned char* right_child = get_page(t->pager, right_pn);
  uint32_t left_pn = get_unused_page_num(t->pager);
  unsigned char* left_child = get_page(t->pager, left_pn);

  memcpy(left_child, root, PAGE_SIZE);
  set_node_root(left_child, 0);

  initialize_inode(root);
  set_node_root(root, 1);
  *inode_num_keys(root) = 1;
  *inode_child(root, 0) = left_pn;
  uint32_t left_child_max_key = get_node_max_key(left_child);
  *inode_key(root, 0) = left_child_max_key;
  *inode_right_child(root) = right_pn;
}

void lnode_split_and_insert(cursor* c, uint32_t key, row* value) {
  unsigned char* old_node = get_page(c->table->pager, c->pagen);
  uint32_t new_page_num = get_unused_page_num(c->table->pager);
  unsigned char* new_node = get_page(c->table->pager, new_page_num);
  initialize_lnode(new_node);

  for (int32_t i = LNODE_MAX_CELLS; i >= 0; i--) {
    unsigned char* dest_node;
    dest_node = i >= LNODE_LEFT_SPLIT_COUNT ? new_node : old_node;
    uint32_t index_within_node = i % LNODE_LEFT_SPLIT_COUNT;
    unsigned char* dest = lnode_cell(dest_node, index_within_node);

    if (i == c->celln) {
      serialize_row(value, dest);
    } else if (i > c->celln) {
      memcpy(dest, lnode_cell(old_node, i - 1), LNODE_CELL_SIZE);
    } else {
      memcpy(dest, lnode_cell(old_node, i), LNODE_CELL_SIZE);
    }
  }
  *(lnode_num_cells(old_node)) = LNODE_LEFT_SPLIT_COUNT;
  *(lnode_num_cells(new_node)) = LNODE_RIGHT_SPLIT_COUNT;

  if (is_node_root(old_node)) {
    create_new_root(c->table, new_page_num);
  } else {
    exit(1);
  }
}

void lnode_insert(cursor* c, uint32_t key, row* value) {
  int i;
  unsigned char* pg = get_page(c->table->pager, c->pagen);
  uint32_t ncells = *lnode_num_cells(pg);

  if (ncells >= LNODE_MAX_CELLS) {
    lnode_split_and_insert(c, key, value);
    return;
  }

  if (c->celln < ncells) {
    for (i = ncells; i > c->celln; i--) {
      memcpy(lnode_cell(pg, i), lnode_cell(pg, i-1), LNODE_CELL_SIZE);
    }
  }

  *(lnode_num_cells(pg)) += 1;
  *(lnode_key(pg, c->celln)) = key;
  serialize_row(value, lnode_value(pg, c->celln));
}

cursor* lnode_find(table* t, uint32_t page_num, uint32_t key) {
  unsigned char* node = get_page(t->pager, page_num);
  uint32_t ncells = *lnode_num_cells(node);

  cursor* c = malloc(sizeof(cursor));
  c->table = t;
  c->pagen = page_num;

  uint32_t min_index = 0;
  uint32_t one_past_max_index = ncells;
  while (one_past_max_index != min_index) {
    uint32_t index = (min_index + one_past_max_index) / 2;
    uint32_t key_at_index = *lnode_key(node, index);
    if (key == key_at_index) {
      c->celln = index;
      return c;
    }
    if (key < key_at_index) {
      one_past_max_index = index;
    } else {
      min_index = index + 1;
    }
  }

  c->celln = min_index;
  return c;
}

node_type get_node_type(unsigned char* node) {
  unsigned char value = *(node + NODE_T_OFFSET);
  return (node_type)value;
}

void set_node_type(unsigned char* node, node_type type) {
  *(node+NODE_T_OFFSET) = (unsigned char)type;
}
