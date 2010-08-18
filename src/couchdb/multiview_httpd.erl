%%----------------------------------------------------------------------------------
%% File    : multiview_httpd.erl
%%
%% Created :  7th July 2010
%% Author : nbarker
%%
%%

-module(multiview_httpd).

-include("couch_db.hrl").

-export([handle_request/2, callback/2]).

-import(couch_httpd, [send_method_not_allowed/2, send_error/4, send_json/3]).

%% Database request handlers
handle_request(#httpd{
        user_ctx=UserCtx,
        method='POST',
        path_parts=[_DbName, _Multi]
}=Req, _Db) ->
 case is_number(couch_httpd:body_length(Req)) of
    false ->
       couch_httpd:send_error(Req, couch_httpd:error_info(bad_request));
    _ ->
      % get the post body
      JsonData = couch_httpd:json_body(Req),
      
      % start the json response
      {ok, Resp} = couch_httpd:start_json_response(Req, 200),
      couch_httpd:send_chunk(Resp, "{\"rows\": ["),
      multiview:multiquery(JsonData, [{user_ctx, UserCtx}], fun ?MODULE:callback/2, {Resp, 0}),
      
      % close
      couch_httpd:send_chunk(Resp, "]}"),
      couch_httpd:end_json_response(Resp)
      
  end;

handle_request(Req, _Db) ->
    send_method_not_allowed(Req, "POST").
 
callback({error, Reason}, {Resp, _}) ->
  ?LOG_ERROR("Error with multiview ~p", [Reason]),
  couch_httpd:send_error(Resp, {bad_request, Reason});

callback(Id, {Resp, Counter}) ->
  case Counter of
     0 ->
        couch_httpd:send_chunk(Resp, binary_to_list(<<"\"", Id/binary, "\"">>));
     _ ->
        couch_httpd:send_chunk(Resp, binary_to_list(<<",\"", Id/binary, "\"">>))
  end,
  {Resp, Counter + 1}.

