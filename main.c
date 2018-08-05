#include <stdlib.h>

#include "src/execute.h"
#include "src/meta.h"
#include "src/prepare.h"
#include "src/readline_hack.h"
#include "src/data.h"

int main(int argc, char* argv[]) {
  char* input;
  statement stmt;
  const char* filename = "db";
  table* t;

  if (argc > 1) filename = argv[1];

  t = db_open(filename);

  while (1) {
    if (!(input = readline("> "))) {
      db_close(t);
      return 0;
    }

    add_history(input);
    if (input[0] == ':') {
      switch (meta(input, t)) {
        case META_UNRECOGNIZED:
          printf("Unrecognized command '%s'.\n", input);
        default:
          break;
      }
    } else {
      switch (prepare_statement(input, &stmt)) {
        case PREP_UNRECOGNIZED:
          printf("Unrecognized keyword at start of '%s'.\n", input);
          break;
        case PREP_SYNTAX_ERROR:
          printf("Syntax error. Could not parse statement '%s'.\n", input);
          break;
        case PREP_STRING_TOO_LONG:
          puts("A string is too long.");
          break;
        case PREP_NEG_ID:
          puts("ID must be positive.");
          break;
        case PREP_SUCCESS:
          switch (execute(&stmt, t)) {
            case EXEC_TABLE_FULL:
              puts("Error: table full!");
              break;
            case EXEC_DUPLICATE_KEY:
              puts("Error: duplicate key!");
              break;
            case EXEC_SUCCESS:
              break;
          }
      }
    }
    free(input);
  }
}
