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

void indent(uint32_t level) {
  int i;
  for (i = 0; i < level; i++) printf("  ");
}

void print_tree(pager* p, uint32_t page_num, uint32_t indent_lvl) {
  void* node = get_page(p, page_num);
  uint32_t num_keys, child;

  switch (get_node_type(node)) {
    case LEAF:
      num_keys = *lnode_num_cells(node);
      indent(indent_lvl);
      printf("- leaf (size %d)\n", num_keys);
      for (uint32_t i = 0; i < num_keys; i++) {
        indent(indent_lvl + 1);
        printf("- %d\n", *lnode_key(node, i));
      }
      break;
    case INTERNAL:
      num_keys = *inode_num_keys(node);
      indent(indent_lvl);
      printf("- internal (size %d)\n", num_keys);
      for (uint32_t i = 0; i < num_keys; i++) {
        child = *inode_child(node, i);
        print_tree(p, child, indent_lvl + 1);

        indent(indent_lvl);
        printf("- key %d\n", *inode_key(node, i));
      }
      child = *inode_right_child(node);
      print_tree(p, child, indent_lvl + 1);
      break;
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
    puts("Tree:");
    print_tree(t->pager, 0, 0);
    return META_SUCCESS;
  }

  if (!strcmp(input, ":d") || !strcmp(input, "dbg")) {
    print_constants();
    puts("\nTree:");
    print_tree(t->pager, 0, 0);
    return META_SUCCESS;
  }

  return META_UNRECOGNIZED;
}
