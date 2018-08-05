#include "data.h"

typedef enum {
  EXEC_SUCCESS,
  EXEC_TABLE_FULL,
  EXEC_DUPLICATE_KEY,
} exec_result;

exec_result execute(statement*, table*);
