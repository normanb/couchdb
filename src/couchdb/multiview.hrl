% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-record(multiview_result, {db = null, id_list, row_count=0, view, req_query,
						    query_args}).
-record(multispatial_result, {db = null, id_list, row_count=0, req_query, 
							  group, index, bbox}).
-record(multiexternal_result, {db = null, id_list, row_count=0}).
-define(MULTI_LIST_ELEMENT, 3).
-define(MULTI_ROWCOUNT_ELEMENT, 4).
-define(MULTI_DB_ELEMENT, 2).


