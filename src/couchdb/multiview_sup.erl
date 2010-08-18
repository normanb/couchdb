%%----------------------------------------------------------------------------------
%% File    : multiview_sup.erl
%%
%% Description : multiview supervisor module
%%
%% Created :  7th July 2010
%% Author : nbarker
%%
%%

-module(multiview_sup).
-export([start_link/0]).
-export([init/1]).

-behaviour(supervisor).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, _Arg = []).
  
init([]) ->
  % ok, RestartStrategy, MaxRestarts, Time
  % Tag, {Mod, Func, ArgList}, Restart, Shutdown, Type, [Mod1]}
  {ok, {{one_for_one, 3, 10},
        [{multiview, {multiview, start_link, []}, permanent, 30000, worker, [multiview]}]}}.  