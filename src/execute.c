#include <stdio.h>
#include <stdlib.h>

#include "execute.h"
#include "serialize.h"

void print_row(row* r) {
  printf("(%d, %s, %s)\n", r->id, r->username, r->email);
}

exec_result execute_insert(statement* stmt, table* t) {
  unsigned char* node = get_page(t->pager, t->root_page_num);
  uint32_t ncells = *lnode_num_cells(node);

  row* row_to_insert = &(stmt->row);
  cursor* c = table_find(t, row_to_insert->id);

  if (c->celln < ncells && row_to_insert->id == *lnode_key(node, c->celln)) {
    return EXEC_DUPLICATE_KEY;
  }

  lnode_insert(c, row_to_insert->id, row_to_insert);

  free(c);

  return EXEC_SUCCESS;
}

exec_result execute_select(statement* sttmt, table* t) {
  row row;
  cursor* c = table_start(t);
  while (!(c->end_of_table)) {
    deserialize_row(cursor_value(c), &row);
    print_row(&row);
    cursor_advance(c);
  }
  free(c);
  return EXEC_SUCCESS;
}

exec_result execute(statement* stmt, table* t) {
  switch (stmt->type) {
    case INSERT:
      return execute_insert(stmt, t);
    case SELECT:
      return execute_select(stmt, t);
      break;
  }
}
