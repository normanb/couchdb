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
        case multiview:multiquery(JsonData, [{user_ctx, UserCtx}],
		    fun ?MODULE:callback/2, {Req, 0}) of
        {HttpReq, 0} ->
            couch_httpd:send_json(HttpReq, ?JSON_DECODE("{\"rows\": []}"));
        {HttpResp, _Counter} ->
            {ok, HttpResp};
         _ ->
            {ok, nil}
        end
    end;
 
handle_request(Req, _Db) ->
    send_method_not_allowed(Req, "POST").
 
callback({error, Reason}, {Req, _}) ->
    % (Req, Code, ErrorStr, ReasonStr)
    couch_httpd:send_error(Req, 500, <<"bad request">>, Reason);

callback({finished, _Reason}, {HttpReqResp, Counter}) ->
    % close
    case Counter of
    0 ->
        {HttpReqResp, 0};
    _ ->
        couch_httpd:send_chunk(HttpReqResp, "]}"),
        couch_httpd:end_json_response(HttpReqResp),
        {HttpReqResp, Counter + 1}
    end;

% depending on whether this is the first callback we will
% have either a HttpReq or an HttpResp object
callback(Id, {HttpReqResp, Counter}) ->
    case Counter of
    0 ->
        {ok, Resp} = couch_httpd:start_json_response(HttpReqResp, 200),
        couch_httpd:send_chunk(Resp, "{\"rows\": [" 
		    ++ ?b2l(<<"\"", Id/binary, "\"">>)),
        {Resp, Counter + 1};
     _ ->
        couch_httpd:send_chunk(HttpReqResp, 
		    ?b2l(<<",\"", Id/binary, "\"">>)),
        {HttpReqResp, Counter + 1}
    end.