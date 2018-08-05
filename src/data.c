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
const uint32_t LNODE_NEXT_LEAF_SIZE = sizeof(uint32_t);
const uint32_t LNODE_NEXT_LEAF_OFFSET =
    LNODE_NUM_CELLS_OFFSET + LNODE_NUM_CELLS_SIZE;
const uint32_t LNODE_HDR_SIZE =
  NODE_HDR_SIZE + LNODE_NUM_CELLS_SIZE + LNODE_NEXT_LEAF_SIZE;

const uint32_t LNODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LNODE_KEY_OFFSET = 0;
const uint32_t LNODE_VALUE_SIZE = sizeof(row);
const uint32_t LNODE_VALUE_OFFSET = LNODE_KEY_OFFSET + LNODE_KEY_SIZE;
const uint32_t LNODE_CELL_SIZE = LNODE_KEY_SIZE + LNODE_VALUE_SIZE;
const uint32_t LNODE_SPACE_FOR_CELLS = PAGE_SIZE - LNODE_HDR_SIZE;
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
const uint32_t INODE_MAX_CELLS = 3;

uint8_t is_node_root(uint8_t* node) {
  return *(node + IS_ROOT_OFFSET);
}

void set_node_root(uint8_t* node, uint8_t is_root) {
  *(node + IS_ROOT_OFFSET) = is_root;
}

uint8_t* get_page(pager* p, uint32_t page_num) {
  if (page_num >= TABLE_MAX_PAGES) {
    printf("Tried to fetch page number out of bounds: %u > %d.\n", page_num,
           TABLE_MAX_PAGES);
    exit(1);
  }

  if (p->pages[page_num] == NULL) {
    uint8_t* page = malloc(PAGE_SIZE);
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

uint8_t* cursor_value(cursor* c) {
  uint32_t page_num = c->pagen;
  uint8_t* page = get_page(c->table->pager, page_num);
  return lnode_value(page, c->celln);
}

void cursor_advance(cursor* c) {
  uint32_t page_num = c->pagen;
  uint8_t* node = get_page(c->table->pager, page_num);

  c->celln += 1;

  if (c->celln >= *lnode_num_cells(node)) {
    uint32_t next_page_num = *lnode_next_leaf(node);
    if (!next_page_num) {
      c->end_of_table = 1;
    } else {
      c->pagen = next_page_num;
      c->celln = 0;
    }
  }
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
  uint8_t* root;
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
  uint8_t* page;
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
  cursor* c = table_find(t, 0);

  uint8_t* node = get_page(t->pager, c->pagen);
  uint32_t ncells = *lnode_num_cells(node);
  c->end_of_table = (ncells == 0);

  return c;
}

cursor* table_find(table* t, uint32_t key) {
  uint32_t root_page_num = t->root_page_num;
  uint8_t* root = get_page(t->pager, root_page_num);

  if (get_node_type(root) == LEAF) {
    return lnode_find(t, root_page_num, key);
  } else {
    return inode_find(t, root_page_num, key);
  }
}

uint32_t* inode_num_keys(uint8_t* node) {
  return (uint32_t*) (node + INODE_NUM_KEYS_OFFSET);
}

void initialize_inode(uint8_t* node) {
  set_node_type(node, INTERNAL);
  set_node_root(node, 0);
  *inode_num_keys(node) = 0;
}

uint32_t* inode_right_child(uint8_t* node) {
  return (uint32_t*)(node + INODE_RIGHT_CHILD_OFFSET);
}

uint32_t* inode_cell(uint8_t* node, uint32_t cell_num) {
  return (uint32_t*)(node + INODE_HDR_SIZE + cell_num * INODE_CELL_SIZE);
}

uint32_t* inode_child(uint8_t* node, uint32_t child_num) {
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

uint32_t* inode_key(uint8_t* node, uint32_t key_num) {
  return (uint32_t*)((uint8_t*)inode_cell(node, key_num) + INODE_CHILD_SIZE);
}

uint32_t inode_find_child(uint8_t* node, uint32_t key) {
  uint32_t num_keys = *inode_num_keys(node);

  uint32_t min_index = 0;
  uint32_t max_index = num_keys;

  while (min_index != max_index) {
    uint32_t index = (min_index + max_index) / 2;
    uint32_t key_to_right = *inode_key(node, index);
    if (key_to_right >= key) {
      max_index = index;
    } else {
      min_index = index + 1;
    }
  }

  return min_index;
}

cursor* inode_find(table* t, uint32_t page_num, uint32_t key) {
  uint8_t* node = get_page(t->pager, page_num);

  uint32_t child_index = inode_find_child(node, key);
  uint32_t child_num = *inode_child(node, child_index);
  uint8_t* child = get_page(t->pager, child_num);
  switch (get_node_type(child)) {
    case LEAF:
      return lnode_find(t, child_num, key);
    case INTERNAL:
      return inode_find(t, child_num, key);
  }
}

uint32_t* lnode_next_leaf(uint8_t* node) {
  return (uint32_t*)(node + LNODE_NEXT_LEAF_OFFSET);
}

uint32_t* lnode_num_cells(uint8_t* node) {
  return (uint32_t*) (node + LNODE_NUM_CELLS_OFFSET);
}

uint8_t* lnode_cell(uint8_t* node, uint32_t cell_num) {
  return node + LNODE_HDR_SIZE + cell_num * LNODE_CELL_SIZE;
}

uint32_t* lnode_key(uint8_t* node, uint32_t cell_num) {
  return (uint32_t*) lnode_cell(node, cell_num);
}

uint8_t* lnode_value(uint8_t* node, uint32_t cell_num) {
  return lnode_cell(node, cell_num) + LNODE_KEY_SIZE;
}

void initialize_lnode(uint8_t* node) {
  *lnode_num_cells(node) = 0;
  *lnode_next_leaf(node) = 0;
  set_node_type(node, LEAF);
  set_node_root(node, 0);
}

uint32_t get_node_max_key(uint8_t* node) {
  switch (get_node_type(node)) {
    case INTERNAL:
      return *inode_key(node, *inode_num_keys(node) - 1);
    case LEAF:
      return *lnode_key(node, *lnode_num_cells(node) - 1);
  }
}

void update_inode_key(uint8_t* node, uint32_t old, uint32_t new) {
  uint32_t old_child_index = inode_find_child(node, old);
  *inode_key(node, old_child_index) = new;
}

uint32_t* node_parent(uint8_t* node) {
  return (uint32_t*)(node + PARENT_PTR_OFFSET);
}

void create_new_root(table* t, uint32_t right_pn) {
  uint8_t* root = get_page(t->pager, t->root_page_num);
  uint8_t* right_child = get_page(t->pager, right_pn);
  uint32_t left_pn = get_unused_page_num(t->pager);
  uint8_t* left_child = get_page(t->pager, left_pn);

  memcpy(left_child, root, PAGE_SIZE);
  set_node_root(left_child, 0);

  initialize_inode(root);
  set_node_root(root, 1);
  *inode_num_keys(root) = 1;
  *inode_child(root, 0) = left_pn;
  uint32_t left_child_max_key = get_node_max_key(left_child);
  *inode_key(root, 0) = left_child_max_key;
  *inode_right_child(root) = right_pn;
  *node_parent(left_child) = t->root_page_num;
  *node_parent(right_child) = t->root_page_num;
}

void lnode_split_and_insert(cursor* c, uint32_t key, row* value) {
  uint8_t* old_node = get_page(c->table->pager, c->pagen);
  uint32_t old_max = get_node_max_key(old_node);
  uint32_t new_page_num = get_unused_page_num(c->table->pager);
  uint8_t* new_node = get_page(c->table->pager, new_page_num);
  initialize_lnode(new_node);
  *node_parent(new_node) = *node_parent(old_node);
  *lnode_next_leaf(new_node) = *lnode_next_leaf(old_node);
  *lnode_next_leaf(old_node) = new_page_num;

  for (int32_t i = LNODE_MAX_CELLS; i >= 0; i--) {
    uint8_t* dest_node;
    dest_node = i >= LNODE_LEFT_SPLIT_COUNT ? new_node : old_node;
    uint32_t index_within_node = i % LNODE_LEFT_SPLIT_COUNT;
    uint8_t* dest = lnode_cell(dest_node, index_within_node);

    if (i == c->celln) {
      serialize_row(value, lnode_value(dest_node, index_within_node));
      *lnode_key(dest_node, index_within_node) = key;
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
    uint32_t parent_page_num = *node_parent(old_node);
    uint32_t new_max = get_node_max_key(old_node);
    uint8_t* parent = get_page(c->table->pager, parent_page_num);

    update_inode_key(parent, old_max, new_max);
    inode_insert(c->table, parent_page_num, new_page_num);
  }
}

void lnode_insert(cursor* c, uint32_t key, row* value) {
  int i;
  uint8_t* pg = get_page(c->table->pager, c->pagen);
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
  uint8_t* node = get_page(t->pager, page_num);
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

node_type get_node_type(uint8_t* node) {
  uint8_t value = *(node + NODE_T_OFFSET);
  return (node_type)value;
}

void set_node_type(uint8_t* node, node_type type) {
  *(node+NODE_T_OFFSET) = (uint8_t)type;
}

void inode_insert(table* t, uint32_t parent_pn, uint32_t child_pn) {
  uint8_t* parent = get_page(t->pager, parent_pn);
  uint8_t* child = get_page(t->pager, child_pn);
  uint32_t child_max_key = get_node_max_key(child);
  uint32_t index = inode_find_child(parent, child_max_key);

  uint32_t original_num_keys = *inode_num_keys(parent);
  *inode_num_keys(parent) = original_num_keys + 1;

  if (original_num_keys >= INODE_MAX_CELLS) {
    printf("Need to implement splitting internal node.\n");
    exit(EXIT_FAILURE);
  }

  uint32_t right_child_pn = *inode_right_child(parent);
  uint8_t* right_child = get_page(t->pager, right_child_pn);

  if (child_max_key > get_node_max_key(right_child)) {
    *inode_child(parent, original_num_keys) = right_child_pn;
    *inode_key(parent, original_num_keys) = get_node_max_key(right_child);
    *inode_right_child(parent) = child_pn;
  } else {
    for (uint32_t i = original_num_keys; i > index; i--) {
      uint8_t* dest = (uint8_t*)inode_cell(parent, i);
      uint8_t* src = (uint8_t*)inode_cell(parent, i - 1);
      memcpy(dest, src, INODE_CELL_SIZE);
    }
    *inode_child(parent, index) = child_pn;
    *inode_key(parent, index) = child_max_key;
  }
}

