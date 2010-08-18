%%----------------------------------------------------------------------------------
%% File    : multiview.erl
%%
%% Description : multiview module to handle the intersection of multiple views
%%
%% Created :  7th July 2010
%%
%% Author : nbarker
%%
%%
-module(multiview).

-include("multiview.hrl").
-include("couch_db.hrl").

-export([multiquery/4]).

-export([start_link/0]).
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2, code_change/3]).

-behaviour(gen_server).

% start / stop, otp callbacks
start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

multiquery(QueryObject, Options, CallBackFunc, CallBackState) when is_tuple(QueryObject) ->
  gen_server:call(?MODULE, {multi_query, QueryObject, Options, CallBackFunc, CallBackState}, infinity).
  
% Functional interface
query_view_count(Query, Options, IncludeId) ->  
  % either design document handlers, db handlers or external
  UrlString = binary_to_list(Query),
  Parts = string:tokens(UrlString, "/"),
  
  case Parts of
    [DbName, "_design", DDocName, "_view", FunctionQueryString] ->
       [Func | QueryParts] = string:tokens(FunctionQueryString, "?&"),
       KVP = lists:map(fun(X) ->
                Idx = string:chr(X, $=),
                {list_to_binary(string:to_lower(string:sub_string(X, 1, Idx-1))), ?JSON_DECODE(string:sub_string(X, Idx+1))}
            end,
            QueryParts),
            
      % open the view 
      {ok, Db} = couch_db:open(list_to_binary(DbName), Options),
              
      {View, _Group} = case couch_view:get_map_view(Db, list_to_binary(["_design/", DDocName]), list_to_binary(Func), nil) of
          {ok, V, G} ->
              {V, G}; 
          {not_found, _Reason} ->
              case couch_view:get_reduce_view(Db, list_to_binary(["_design/", DDocName]), list_to_binary(Func), nil) of
                {ok, ReduceView, G} ->
                   % we need the ids so get the map view
                   {couch_view:extract_map_view(ReduceView), G};
                _ ->
                  {null, null}
              end
       end,

       {{RowCount, IdAcc}, QueryArgs} = case View of 
          null ->
            {0, null};
          _ ->
            Args = case proplists:get_value(<<"startkey">>, KVP, undefined) of
                undefined ->
                    #view_query_args{};
                _ ->
                    #view_query_args{
                        start_key = proplists:get_value(<<"startkey">>, KVP, undefined), 
                        end_key = proplists:get_value(<<"endkey">>, KVP, undefined)
                    }
            end,
            
            FoldAccInit = {Args#view_query_args.limit, Args#view_query_args.skip, undefined, {0, []}},            
            
            FoldlFun = fun({{_Key, DocId}, _Value}, _OffsetReds, {AccLimit, AccSkip, Resp, {Counter, Acc}=State}) ->
                case {AccLimit, AccSkip, Resp} of
                    {0, _, _} ->
                        % we've done "limit" rows, stop foldling
                        {stop, {0, 0, undefined, State}};
                    {_, AccSkip, _} when AccSkip > 0 ->
                        % just keep skipping
                        {ok, {AccLimit, AccSkip - 1, undefined, State}};
                    {_, _, _} ->
                        % count the row
                        case IncludeId of
                           true ->
                            {ok, {AccLimit - 1, 0, undefined, {Counter + 1, [[DocId]|Acc]}}};
                           _ ->
                            {ok, {AccLimit - 1, 0, undefined, {Counter + 1, Acc}}}
                        end
                end
            end,
            
            {ok, _LastReduce,  {_, _, _, State}} = couch_view:fold(View, FoldlFun, FoldAccInit, couch_httpd_view:make_key_options(Args)),
            {State, Args}
       end,
       #multiview_result{db=Db, view=View, row_count=RowCount, id_list=lists:umerge(IdAcc), req_query=Query, query_args=QueryArgs};
    [DbName, "_design", DDocName, "_spatial", FunctionQueryString] ->
        % handle spatial as a special case
        {ok, Db} = couch_db:open(list_to_binary(DbName), Options),
        
        % parse FunctionQueryString for SpatialName and Stale parameter
        [SpatialName | QueryParts] = string:tokens(FunctionQueryString, "?&"),
        KVP = lists:map(fun(X) ->
                Idx = string:chr(X, $=),
                {list_to_binary(string:to_lower(string:sub_string(X, 1, Idx-1))), string:sub_string(X, Idx+1)}
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
            Db, list_to_binary("_design/" ++ DDocName), list_to_binary(SpatialName), Stale),
            
        % create a FoldFun and FoldAccInit
        FoldAccInit = {undefined, {0, []}},
        FoldFun =  fun({_Bbox, DocId, _Value}, {undefined, {Counter, Acc}}) ->
            case IncludeId of
               true ->
                 {ok, {undefined, {Counter + 1, [[DocId]|Acc]}}};
               _ ->
                 {ok, {undefined, {Counter + 1, Acc}}}
            end
        end,        
            
        {ok, {_, {RowCount, Acc}}} = couch_spatial:fold(Group, Index, FoldFun, FoldAccInit, Bbox),    
       
        #multispatial_result{db=Db, row_count=RowCount, id_list=lists:umerge(Acc), req_query=Query, group=Group, index=Index, bbox=Bbox};
    [Db, ExternalFunction, FunctionQueryString] ->
       % external functions can do what they like, as a particular case we handle FTI
       case ExternalFunction of
         "_fti" ->
            [Func | QueryParts] = string:tokens(FunctionQueryString, "?&"),
            KVP = lists:map(fun(X) ->
                    Idx = string:chr(X, $=),
                    {list_to_binary(string:sub_string(X, 1, Idx-1)), list_to_binary(string:sub_string(X, Idx+1))}
                end,
                QueryParts),
         
            % e.g. by_title?q=mask*
            JsonRequest = {[{<<"path">>,[list_to_binary(Db), <<"_fti">>, list_to_binary(Func)]},
                            {<<"query">>,{KVP}}]},
                            
            Result = couch_external_manager:execute("fti", JsonRequest),

            Rows = try couch_util:get_nested_json_value(Result, [<<"json">>, <<"rows">>])
            catch
              throw: {not_found, _} -> #multiexternal_result{}
            end,
            
            case Rows of
              [] ->
                #multiexternal_result{};
              _ ->
                Ids = lists:map(fun(R) ->
                        try couch_util:get_nested_json_value(R, [<<"id">>])
                        catch
                          throw: {not_found, _} -> []
                        end
                     end,
                     Rows),
                
                 % review this, should we try an paginate external fti server?
                 % put the result in a form suitable for umerge
                 #multiexternal_result{row_count=length(Ids), id_list=lists:umerge([[Id] || Id <- Ids])}
            end;
          _ ->
           #multiexternal_result{}
       end;
    _ ->
      % no match return 0
      {view, nil, 0, Query}
  end.
  
multi_query(QueryObject, Options, CallBackFunc, CallBackState) ->
  % in parallel fetch the results from CouchDB and in parallel do a unique sort and merge
  % TODO parse limit, start, offset, include_docs
  
  % get the list of queries
  Queries = try couch_util:get_nested_json_value(QueryObject, [<<"views">>])
    catch
      throw: {not_found, _} -> []
    end,
  
  % use pmap on the query list to fetch the total number of counts per query
  case length(Queries) of
    1 ->
      % querying a single view, no intersection required
      [X] = Queries,
      ResultRec = query_view_count(X, Options, true),
      Ids = erlang:element(?MULTI_LIST_ELEMENT, ResultRec),
      write_single_response(Ids, CallBackFunc, CallBackState);
   _ ->
      [Start | Rest] = ResultList = lists:keysort(?MULTI_ROWCOUNT_ELEMENT, pmap:pmap(fun(X) -> query_view_count(X, Options, false)  end, Queries)),
      case erlang:element(?MULTI_ROWCOUNT_ELEMENT, Start) == 0 of
        true ->
          ok;
        _ ->
          couch_query_ring:start(Start, Rest, CallBackFunc, CallBackState)
      end,
      
      % close all the open db connections (just references), might as well do it in parallel
      pmap:pmap(fun(Record) -> 
            Db = erlang:element(?MULTI_DB_ELEMENT, Record),
            case Db of 
              null -> 
                ok;
              _ -> 
                couch_db:close(Db)
            end
        end, 
        ResultList)
  end.
  
write_single_response([], _CallBackFunc, _CallBackState) ->
  [];

write_single_response([Id | Rem], CallBackFunc, CallBackState) ->
  NewState = CallBackFunc(Id, CallBackState),
  write_single_response(Rem, CallBackFunc, NewState).
  
% Callback functions
init(_Args) ->
   {ok, []}.
   
terminate(_Reason, _State) ->
  ok.
  
handle_info(_Info, N) -> {noreply, N}.

handle_cast(_Msg, N) -> {noreply, N}.

code_change(_OldVsn, N, _Extra) -> {ok, N}.

handle_call({multi_query, QueryObject, Options, CallBackFunc, CallBackState}, _From, State) ->
    {reply, multi_query(QueryObject, Options, CallBackFunc, CallBackState), State}.
  
 
