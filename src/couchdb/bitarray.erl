%% ``The contents of this file are subject to the Erlang Public License,
%% Version 1.1, (the "License"); you may not use this file except in
%% compliance with the License. You should have received a copy of the
%% Erlang Public License along with this software. If not, it can be
%% retrieved via the world wide web at http://www.erlang.org/.
%% 
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and limitations
%% under the License.
%% 
%% Based on
%% Scalable Bloom Filters
%% Paulo Sérgio Almeida, Carlos Baquero, Nuno Preguiça, David Hutchison
%% Information Processing Letters
%% Volume 101, Issue 6, 31 March 2007, Pages 255-261 
%%
%% Provides scalable bloom filters that can grow indefinitely while
%% ensuring a desired maximum false positive probability. Also provides
%% standard partitioned bloom filters with a maximum capacity. Bit arrays
%% are dimensioned as a power of 2 to enable reusing hash values across
%% filters through bit operations. Double hashing is used (no need for
%% enhanced double hashing for partitioned bloom filters).
%%
-module(bitarray).
-export([new/1, set/2, get/2]).

-define(W, 24).

new(N) -> array:new((N-1) div ?W + 1, {default, 0}).

set(I, A) ->
  AI = I div ?W,
  V = array:get(AI, A),
  V1 = V bor (1 bsl (I rem ?W)),
  array:set(AI, V1, A).

get(I, A) ->
  AI = I div ?W,
  V = array:get(AI, A),
  V band (1 bsl (I rem ?W)) =/= 0.

