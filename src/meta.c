#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "meta.h"

void print_constants() {
  puts("Constants:");
  printf("ROW_SIZE: %ld\n", sizeof(row));
  printf("NODE_HDR_SIZE: %d\n", NODE_HDR_SIZE);
  printf("LNODE_HEADER_SIZE: %d\n", LNODE_HEADER_SIZE);
  printf("LNODE_CELL_SIZE: %d\n", LNODE_CELL_SIZE);
  printf("LNODE_SPACE_FOR_CELLS: %d\n", LNODE_SPACE_FOR_CELLS);
  printf("LNODE_MAX_CELLS: %d\n", LNODE_MAX_CELLS);
}

void print_leaf_node(void* node) {
  uint32_t num_cells = *lnode_num_cells(node);
  puts("Tree:");
  printf("leaf (size %d)\n", num_cells);
  for (uint32_t i = 0; i < num_cells; i++) {
    printf("  - %d : %d\n", i, *lnode_key(node, i));
  }
}

meta_result meta(char* input, table* t) {
  if (!strcmp(input, ":q")) {
    db_close(t);
    puts("Goodbye!");
    exit(0);
  }

  if (!strcmp(input, ":c")) {
    print_constants();
    return META_SUCCESS;
  }

  if (!strcmp(input, ":tree")) {
    print_leaf_node(get_page(t->pager, 0));
    return META_SUCCESS;
  }

  if (!strcmp(input, ":d") || !strcmp(input, "dbg")) {
    print_constants();
    puts("");
    print_leaf_node(get_page(t->pager, 0));
    return META_SUCCESS;
  }

  return META_UNRECOGNIZED;
}
