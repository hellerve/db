#include "data.h"

typedef enum {
  EXEC_SUCCESS,
  EXEC_TABLE_FULL,
} exec_result;

exec_result execute(statement*, table*);
