-record(multiview_result, {db = null, id_list, row_count=0, view, req_query, query_args}).
-record(multispatial_result, {db = null, id_list, row_count=0, req_query, group, index, bbox}).
-record(multiexternal_result, {db = null, id_list, row_count=0}).
-define(MULTI_LIST_ELEMENT, 3).
-define(MULTI_ROWCOUNT_ELEMENT, 4).
-define(MULTI_DB_ELEMENT, 2).


