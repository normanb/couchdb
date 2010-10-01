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

-module(multiview).

-include("multiview.hrl").
-include("couch_db.hrl").

-export([multiquery/4]).

multiquery(QueryObject, Options, CallBackFunc, CallBackState) ->
    % TODO parse limit, start, offset, include_docs
    % get the list of queries
    Queries = try couch_util:get_nested_json_value(QueryObject, [<<"views">>])
    catch
        throw: {not_found, _} -> []
    end,
  
    % use query list to fetch the total number of counts per query
    case length(Queries) of
    1 ->
        % querying a single view, no intersection required
        [Q] = Queries,
        %ResultRec = query_view_count(X, Options),
	    % Ids = erlang:element(?MULTI_LIST_ELEMENT, ResultRec),
	    % query and write single view
	    CallBackFunc({finished, []}, 
		    stream_view_intersect(Q, Options, [],
			CallBackFunc, CallBackState));
    _ ->
        [Start|Rem]= lists:keysort(?MULTI_ROWCOUNT_ELEMENT,
		    lists:map(fun(X) -> 
			    query_view_count(X, Options)
				end, Queries)),
      
        case erlang:element(?MULTI_ROWCOUNT_ELEMENT, Start) == 0 of
        true ->
            CallBackState;
        _ ->
		    % query each bloom filter for an intersection by folding over the
		    % smallest view
		    % TODO for disjoint we will use the largest view and test for 
		    % not being a view member
		    NewCallBackState = case Start#bloom_view_result.id_list of
			null ->
			    % fold over the smallest view
				stream_view_intersect(Start#bloom_view_result.q, Options, Rem, 
				    CallBackFunc, CallBackState);
			_ ->
			    intersect_and_stream(Start#bloom_view_result.id_list, Rem,
				    CallBackFunc, CallBackState)
            end,
		    CallBackFunc({finished, []}, NewCallBackState)
        end
    end.

query_view_count(Query, Options) ->  
    % either design document handlers, db handlers or external
    UrlString = ?b2l(Query),
    Parts = string:tokens(UrlString, "/"),
    case Result = query_view(Query, Parts, Options) of 
	nil ->
        % no match return 0
        {view, nil, [], 0, Query};
	_ ->
        Result
    end.

stream_view_intersect(Query, Options, Filters, CallBackFunc, CallBackState) ->
    UrlString = ?b2l(Query),
    Parts = string:tokens(UrlString, "/"),

    case Parts of
	[DbName, "_design", DDocName, "_view", FunctionQueryString] ->
	    [Func | QueryParts] = string:tokens(FunctionQueryString, "?&"),
		{Db, View, Args} = get_db_args_view(DbName, DDocName, Func, 
		    QueryParts, Options), 

		FoldAccInit = {Args#view_query_args.limit, Args#view_query_args.skip,
							undefined, CallBackState},
		  
      	FoldlFun = fun({{_Key, DocId}, _Value}, _OffsetReds,
		    {AccLimit, _AccSkip, _Resp, CBState}) ->
            {ok, {AccLimit - 1, 0, undefined,
			    intersect_and_stream([DocId], Filters,
				CallBackFunc, CBState)}}
			end,

        {ok, _LastReduce, {_, _, _, NewCallBackState}} = 
		    couch_view:fold(View, FoldlFun,
			FoldAccInit, 
			couch_httpd_view:make_key_options(Args)),
		  
		couch_db:close(Db),
	    NewCallBackState;
    [_DbName, "_design", _DDocName, "_spatial", _FunctionQueryString] ->
	    % stream spatial results
		query_spatial_view(Query, Parts, Options, true,
		    Filters, CallBackFunc, CallBackState);
   	_ ->
	    CallBackState
    end.

query_view(Query, [DbName, "_design", DDocName, "_view",
    FunctionQueryString], Options) ->
    [Func | QueryParts] = string:tokens(FunctionQueryString, "?&"),
 
    {Db, View, Args} = get_db_args_view(DbName, DDocName, 
	    Func, QueryParts, Options), 
  
% create a bloom filter
% using reduce results in duplicates, {dups, [...]}
% if expand_dups works then we will go back to this approach, for now fold
%  {ok, {{RowCount, _}, BloomFilter}} = couch_btree:fold_reduce(View#view.btree, 
% 	fun(Start, PartialReds, Filter) ->
%	    NewBloomFilter = case PartialReds of
%		  {KeyList, _} ->
%			% call View:expand_dups as needed
%		    add_to_filter(KeyList, Filter);
%		  _ ->
%            Filter
%        end,
%	  
%	    {ok, {couch_btree:final_reduce(
%	      View#view.btree, 
%	  	  PartialReds), NewBloomFilter}}
%	  end,
%	  bloom:sbf(?BLOOM_FILTER_SIZE), couch_httpd_view:make_key_options(Args)), 
    FoldAccInit = {Args#view_query_args.limit, Args#view_query_args.skip,
	    undefined, {0, bloom:sbf(?BLOOM_FILTER_SIZE)}},
    FoldlFun = fun({{_Key, DocId}, _Value}, _OffsetReds,
	    {AccLimit, _AccSkip, _Resp, {Counter, Filter}}) ->
      	{ok, {AccLimit - 1, 0, undefined, 
		{Counter + 1, add_to_filter([DocId], Filter)}}}
    end,
    
    {ok, _LastReduce, {_, _, _, {RowCount, BloomFilter}}} =
	    couch_view:fold(View, FoldlFun,
		FoldAccInit, couch_httpd_view:make_key_options(Args)),

	couch_db:close(Db),
    #bloom_view_result{q=Query, options=Options, 
	row_count=RowCount, bloom_filter=BloomFilter};

% make spatial a special case
query_view(Query, [DbName, "_design", DDocName, "_spatial", 
	FunctionQueryString], Options) ->
    query_spatial_view(Query, [DbName, "_design", DDocName, "_spatial", 
	    FunctionQueryString], Options, false, [], nil, nil);

% java lucene fti
query_view(Query, [Db, Handler, "_design", DDoc, FunctionQueryString], Options) ->
    case Handler of 
	"_fti" ->
		% query external fti handler
		[Func | QueryParts] = string:tokens(FunctionQueryString, "?&"),
		KVP = lists:map(fun(X) ->
            Idx = string:chr(X, $=),
            {?l2b(string:sub_string(X, 1, Idx-1)), 
			?l2b(string:sub_string(X, Idx+1))}
        end,
        QueryParts),
		JsonRequest= {[{<<"method">>, <<"GET">>}, {<<"path">>, [?l2b(Db), <<"_fti">>,
		    <<"_design">>, ?l2b(DDoc), ?l2b(Func)]},
			{<<"query">>,{KVP}}]},

		Output = couch_external_manager:execute("fti", JsonRequest),
        
		ResultStr = try couch_util:get_nested_json_value(Output, 
		    [<<"body">>])
        catch
            throw: {not_found, _} -> 
			    []
        end,
       
		Result = ?JSON_DECODE(?b2l(ResultStr)),
		Rows =  try couch_util:get_nested_json_value(Result, 
		    [<<"rows">>])
        catch
            throw: {not_found, _} -> 
			    []
        end,
		 
        case Rows of
        [] ->
		    #bloom_view_result{q=Query, options=Options};
        _ ->
            Ids = [couch_util:get_nested_json_value(R, [<<"id">>]) ||
			    R <- Rows],
		    #bloom_view_result{q=Query, options=Options, row_count=length(Ids),
				bloom_filter=add_to_filter(Ids, bloom:sbf(?BLOOM_FILTER_SIZE)),
				id_list=Ids}
        end;
	_ ->
	    #bloom_view_result{q=Query, options=Options}
	end;

% TODO dump this function and amalgamate with java lucene
query_view(Query, [Db, ExternalFunction, FunctionQueryString], Options) ->
    % external functions can do what they like, 
    % as a particular case we handle CLucene FTI	
    case ExternalFunction of
    % clucene fti
    "_fti" ->
        [Func | QueryParts] = string:tokens(FunctionQueryString, "?&"),
        KVP = lists:map(fun(X) ->
            Idx = string:chr(X, $=),
            {?l2b(string:sub_string(X, 1, Idx-1)), 
			?l2b(string:sub_string(X, Idx+1))}
        end,
        QueryParts),
         
        % e.g. by_title?q=mask*
        JsonRequest = {[{<<"path">>,[?l2b(Db), <<"_fti">>, 
		?l2b(Func)]},
        {<<"query">>,{KVP}}]},
                            
        Result = couch_external_manager:execute("fti", JsonRequest),
		
        Rows = try couch_util:get_nested_json_value(Result, 
		    [<<"json">>, <<"rows">>])
        catch
            throw: {not_found, _} -> 
			    []
        end,
            
        case Rows of
        [] ->
		    #bloom_view_result{q=Query, options=Options};
        _ ->
            Ids = [couch_util:get_nested_json_value(R, [<<"id">>]) ||
			    R <- Rows],
		    #bloom_view_result{q=Query, options=Options, row_count=length(Ids),
				bloom_filter=add_to_filter(Ids, bloom:sbf(?BLOOM_FILTER_SIZE)),
				id_list=Ids}
        end;
    _ ->
	    #bloom_view_result{q=Query, options=Options}
    end;

query_view(_Query, _, _Options) ->
    nil.

% spatial query and streaming - special case
query_spatial_view(Query, [DbName, "_design", DDocName, "_spatial", 
    FunctionQueryString], Options, DoStream,
	Filters, CallBackFunc, CallBackState) ->
    % handle spatial as a special case
    {ok, Db} = couch_db:open(?l2b(DbName), Options),
        
    % parse FunctionQueryString for SpatialName and Stale parameter
    [SpatialName | QueryParts] = string:tokens(FunctionQueryString, "?&"),
    KVP = lists:map(fun(X) ->
        Idx = string:chr(X, $=),
        {?l2b(string:to_lower(
		    string:sub_string(X, 1, Idx-1))), 
			string:sub_string(X, Idx+1)}
        end,
        QueryParts),
        
    Bbox = case lists:keyfind(<<"bbox">>, 1, KVP) of
    {_, Val} ->   
        list_to_tuple(?JSON_DECODE("[" ++ Val ++ "]"));
    _ ->
        []
    end,

    Stale = case lists:keyfind("stale", 1, KVP) of
    {_, StaleVal} ->
        StaleVal;
    _ ->
        nil
    end,
        
    {ok, Index, Group} = couch_spatial:get_spatial_index(
        Db, ?l2b("_design/" ++ DDocName), 
		?l2b(SpatialName), Stale),
            
    % create a FoldFun and FoldAccInit
    case DoStream of 
	true ->
	    FoldAccInit = {undefined, CallBackState},
        FoldFun = fun({_Bbox, DocId, _Value}, {undefined, CBState}) ->
            {ok, {undefined, intersect_and_stream([DocId], Filters,
			CallBackFunc, CBState)}}
        end,
        
        {ok, {_, NewCallBackState}} = couch_spatial:fold(Group, Index, FoldFun,
			FoldAccInit, Bbox), 
		couch_db:close(Db),
		NewCallBackState;
	_ ->
        FoldAccInit = {undefined, {0, bloom:sbf(?BLOOM_FILTER_SIZE), []}},
        FoldFun =  fun({_Bbox, DocId, _Value}, 
		    {undefined, {Counter, Filter, Acc}}) ->
            {ok, {undefined, {Counter + 1, bloom:add(DocId, Filter), 
			[DocId|Acc]}}}
        end,        
            
        {ok, {_, {RowCount, BloomFilter, Acc}}} = couch_spatial:fold(Group, 
		Index, FoldFun, FoldAccInit, Bbox),    
   
        couch_db:close(Db),
        #bloom_view_result{q=Query, options=Options, 
		    row_count=RowCount, bloom_filter=BloomFilter,
			id_list=Acc}
    end.

get_db_args_view(DbName, DDocName, Func, QueryParts, Options) ->
    KVP = lists:map(fun(X) ->
        Idx = string:chr(X, $=),
        {?l2b(string:to_lower(string:sub_string(X, 1, Idx-1))),
		?JSON_DECODE(string:sub_string(X, Idx+1))}
        end,
        QueryParts),
  
    {ok, Db} = couch_db:open(?l2b(DbName), Options),
  
    {View, _Group} = case couch_view:get_map_view(Db, 
	    ?l2b(["_design/", DDocName]),
	    ?l2b(Func), nil) of
    {ok, V, G} ->
        {V, G}; 
    {not_found, _Reason} ->
        case couch_view:get_reduce_view(Db, 
		    ?l2b(["_design/", DDocName]),
			?l2b(Func), nil) of
        {ok, ReduceView, G} ->
            % get the map view
            {couch_view:extract_map_view(ReduceView), G};
        _ ->
            {null, null}
        end
    end,
  
    Args = case proplists:get_value(<<"startkey">>, KVP, undefined) of
    undefined ->
        #view_query_args{};
    _ ->
        #view_query_args{
            start_key = proplists:get_value(<<"startkey">>, KVP, undefined), 
            end_key = proplists:get_value(<<"endkey">>, KVP, undefined)
        }
    end,
    {Db, View, Args}.

add_to_filter([{{_Key, Id}, _Val}|Rem], BloomFilter) -> 
    NewBloomFilter = bloom:add(Id, BloomFilter),
    add_to_filter(Rem, NewBloomFilter);

add_to_filter([], BloomFilter) ->
	BloomFilter;

add_to_filter([Id | Rem], BloomFilter) ->
    NewBloomFilter = bloom:add(Id, BloomFilter),
    add_to_filter(Rem, NewBloomFilter);

add_to_filter(_, BloomFilter) ->
    BloomFilter.

intersect_and_stream([], _Filters, _CallBackFunc, CallBackState) ->
	CallBackState;

intersect_and_stream([Id | Rem], [], CallBackFunc, CallBackState) ->
	intersect_and_stream(Rem, [], CallBackFunc,
	    stream_result(Id, CallBackFunc, CallBackState));

intersect_and_stream([Id | Rem], Filters, CallBackFunc, CallBackState) ->
    case intersection(Id, Filters) of
	true ->
	    NewCallBackState = stream_result(Id, CallBackFunc, CallBackState),
		intersect_and_stream(Rem, Filters, CallBackFunc, NewCallBackState);
    _ ->
        CallBackState
    end.

intersection(_DocId, []) ->
	true;

intersection(DocId, [#bloom_view_result{bloom_filter = Filter} | Rem]) ->
    case bloom:member(DocId, Filter) of
    true ->
        intersection(DocId, Rem);
    _ ->
        false
    end.

stream_result([], _CallBackFunc, CallBackState) ->
    CallBackState;

stream_result([DocId|Rem], CallBackFunc, CallBackState) ->
    stream_result(Rem, CallBackFunc, CallBackFunc(DocId, CallBackState));

stream_result(DocId, CallBackFunc, CallBackState) ->
    CallBackFunc(DocId, CallBackState).
