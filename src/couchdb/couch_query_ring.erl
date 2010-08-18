%
% File: couch_query_ring.erl
%
% Description: ring processing network topology
%
% send an id from the start list to the next node in the ring, if the id is in adjacent node then the this node sends to the next ring node ....
% if the id gets all round the ring and back to the start node then is has intersected all queries and should be included. The nodes in the ring 
% should be sorted in size from small to large for this to be effective
%
% In addition send the initial id list round in parallel
%
% Note: an alternative model is to blast all nodes with the same id and collect the responses, this seems a bit brute force
% but might be an interesting comparison
%
% Created: 20th July 2010
%
% Author : nbarker
%
-module(couch_query_ring).

-export([start/4, loop/4, ring/3]).

-include("multiview.hrl").
-include("couch_db.hrl").
    
ring([QueryReq | Rem], From, Main) ->
    case Rem of 
      [] ->
        From ! {connect, self()},
        loop(self(), Main, Main, QueryReq);
      _ ->
        process_flag(trap_exit, true),
        spawn(couch_query_ring, ring, [Rem, self(), Main]),
        receive
          {connect, To} ->
            From ! {connect, self()},
            loop(self(), To, Main, QueryReq)
        end
    end.
    
loop(From, To, Main, #multiview_result{view=View, query_args=Args} = QueryRec) when is_record(QueryRec, multiview_result) ->
  receive
    {'EXIT', SomePid, Reason} ->
      Main !  {'EXIT', SomePid, Reason}; 
    die ->
      To ! die;      
    {_Pid, QueryId} ->
      % pass the message if there is an intersection
      FoldAccInit = {Args#view_query_args.limit, Args#view_query_args.skip, undefined, []},            
      FoldlFun = fun({{_Key, DocId}, _Value}, _OffsetReds, {AccLimit, _AccSkip, _Resp, Acc}) ->
            case DocId == QueryId of
              true ->
                {stop, {0, 0, undefined, [DocId|Acc]}};
              _ ->
                {ok, {AccLimit - 1, 0, undefined, Acc}}
            end
      end,
      
      {ok, _LastReduce,  {_, _, _, Acc}} = couch_view:fold(View, FoldlFun, FoldAccInit, 
            couch_httpd_view:make_key_options(Args)),
      
      case length(Acc) of
        0 ->
          Main ! {self(), []};
        _ ->
          To ! {self(), QueryId}
      end,
      loop(From, To, Main, QueryRec)
  end;

loop(From, To, Main, #multispatial_result{group=Group, index=Index, bbox=Bbox} = QueryRec) when is_record(QueryRec, multispatial_result) ->
  receive
    {'EXIT', SomePid, Reason} ->
        Main !  {'EXIT', SomePid, Reason};   
    die ->
      To ! die;        
    {_Pid, QueryId} ->
       % pass the message if there is an intersection
        % create a FoldFun and FoldAccInit
        FoldAccInit = {undefined, []},
        FoldFun =  fun({_Bbox, DocId, _Value}, {undefined, Acc}) ->
            case DocId == QueryId of
              true ->
                {stop, {undefined, [DocId|Acc]}};
              _ ->
                {ok, {undefined, Acc}}
            end
        end,        
        
        {ok, {_, Acc}} = couch_spatial:fold(Group, Index, FoldFun, FoldAccInit, Bbox),      
      
        case length(Acc) of
          0 ->
            Main ! {self(), []};
          _ ->
            To ! {self(), QueryId}
        end,
        loop(From, To, Main, QueryRec)    
  end;
  
loop(From, To, Main, #multiexternal_result{id_list=IdList} = QueryRec) when is_record(QueryRec, multiexternal_result) ->
  receive
    {'EXIT', SomePid, Reason} ->
       Main !  {'EXIT', SomePid, Reason};  
    die ->
      To ! die;
    {_Pid, QueryId} ->
      % pass the message if there is an intersection
      case lists:member(QueryId, IdList) of
        true ->
          To ! {self(), QueryId};
        _ ->
          Main ! {self(), []}
      end,
      loop(From, To, Main, QueryRec)
  end.  

gather(PID, CallBackFunc, CallBackState, {Counter, StopCount}) when Counter == StopCount ->
  receive
    {'EXIT', _SomePid, Reason} ->
       %% do somewith error
      CallBackFunc({error, Reason}, CallBackState);
    die ->
      ok;  
    {_Pid, Id} ->
      case Id of 
         [] ->
           ok;
         _ ->
          CallBackFunc(Id, CallBackState)
      end,
      PID ! die,
      gather(PID, CallBackFunc, CallBackState, {Counter, StopCount})
  end;

gather(PID, CallBackFunc, CallBackState, {Counter, StopCount}) ->
  receive
    {'EXIT', _SomePid, Reason} ->
      ?LOG_ERROR("Error with multiview ~p", [Reason]),
      CallBackFunc({error, "Multiview process has died"}, CallBackState);
    {_Pid, Id} ->
      NewCallbackState = case Id of
        [] ->
          CallBackState;
        _ ->
          CallBackFunc(Id, CallBackState)
      end,      
      gather(PID, CallBackFunc, NewCallbackState, {Counter + 1, StopCount})
  end.
  
start(StartNode, Nodes, CallBackFunc, CallBackState) when is_tuple(StartNode) ->
   process_flag(trap_exit, true),
   spawn(couch_query_ring, ring, [Nodes, self(), self()]),
   receive
     {connect, First} ->
      % queue up the messaages on the first node 
       case erlang:element(1, StartNode) of
          multiview_result ->
            % -record(multiview_result, {db = null, row_count=0, req_query, query_args}).
            % set RowFunAcc to undefined, leave limit skip etc as may be used later
            QueryArgs = StartNode#multiview_result.query_args,
            Limit = QueryArgs#view_query_args.limit,
            Skip = QueryArgs#view_query_args.skip,
            
            FoldAccInit = {Limit, Skip, undefined, undefined},            
            FoldlFun = fun({{_Key, DocId}, _Value}, _OffsetReds, {AccLimit, AccSkip, Resp, RowFunAcc}) ->
                case {AccLimit, AccSkip, Resp} of
                    {0, _, _} ->
                        % we've done "limit" rows, stop foldling
                        {stop, {0, 0, Resp, RowFunAcc}};
                    {_, AccSkip, _} when AccSkip > 0 ->
                        % just keep skipping
                        {ok, {AccLimit, AccSkip - 1,  Resp, RowFunAcc}};
                    {_, _, _} ->
                        % rendering the row
                        First ! {self(), DocId},
                        {ok, {AccLimit - 1, 0,  Resp, RowFunAcc}}
                end
            end,
            couch_view:fold(StartNode#multiview_result.view, FoldlFun, FoldAccInit, couch_httpd_view:make_key_options(StartNode#multiview_result.query_args));
          multispatial_result ->
            % -record(multispatial_result, {db = null, row_count=0, req_query, group, index, bbox}).
            FoldAccInit = {undefined, []},
            FoldFun =  fun({_Bbox, DocId, _Value}, {undefined, Acc}) ->
                % don't accumulate
                First ! {self(), DocId},
                {ok, {undefined, Acc}}
            end,        
            couch_spatial:fold(StartNode#multispatial_result.group, StartNode#multispatial_result.index, FoldFun, FoldAccInit, StartNode#multispatial_result.bbox);
          multiexternal_result ->
           % -record(multiexternal_result, {db = null, row_count=0, id_list}).
            lists:map(fun(X) -> First ! {self(), X} end, StartNode#multiexternal_result.id_list);
          View ->
            V = list_to_binary(atom_to_list(View)),
            CallBackFunc({error, <<"unsupported view in multiview ",  V/binary>>}, CallBackState)
      end,       
      StopCount = erlang:element(?MULTI_ROWCOUNT_ELEMENT, StartNode),      
      gather(First, CallBackFunc, CallBackState, {1, StopCount})
   end.
   
   
   
 