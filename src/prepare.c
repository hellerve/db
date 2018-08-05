#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "prepare.h"

prep_result prepare_insert(char* input, statement* stmt) {
  stmt->type = INSERT;

  strtok(input, " ");
  char* id_string = strtok(NULL, " ");
  char* username = strtok(NULL, " ");
  char* email = strtok(NULL, " ");

  if (!id_string || !username || !email) return PREP_SYNTAX_ERROR;

  int id = atoi(id_string);

  if (id < 1) return PREP_NEG_ID;

  if (strlen(username) > 32) return PREP_STRING_TOO_LONG;
  if (strlen(email) > 255) return PREP_STRING_TOO_LONG;

  stmt->row.id = id;
  strcpy(stmt->row.username, username);
  strcpy(stmt->row.email, email);

  return PREP_SUCCESS;
}

prep_result prepare_statement(char* input, statement* stmt) {
  if (!strncmp(input, "insert", 6)) return prepare_insert(input, stmt);

  if (!strncmp(input, "select", 6)) {
    stmt->type = SELECT;
    return PREP_SUCCESS;
  }

  return PREP_UNRECOGNIZED;
}
