#include "data.h"

typedef enum {
  PREP_SUCCESS,
  PREP_SYNTAX_ERROR,
  PREP_UNRECOGNIZED,
  PREP_STRING_TOO_LONG,
  PREP_NEG_ID,
} prep_result;

prep_result prepare_statement(char*, statement*);
