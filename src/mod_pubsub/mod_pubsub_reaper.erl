%%% ====================================================================
%%% ``The contents of this file are subject to the Erlang Public License,
%%% Version 1.1, (the "License"); you may not use this file except in
%%% compliance with the License. You should have received a copy of the
%%% Erlang Public License along with this software. If not, it can be
%%% retrieved via the world wide web at http://www.erlang.org/.
%%%
%%% Software distributed under the License is distributed on an "AS IS"
%%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%%% the License for the specific language governing rights and limitations
%%% under the License.
%%%
%%% The Initial Developer of the Original Code is ProcessOne.
%%% Portions created by ProcessOne are Copyright 2006-2009, ProcessOne
%%% All Rights Reserved.''
%%% This software is copyright 2006-2009, ProcessOne.
%%%
%%% @author Brian Cully <bjc@kublai.com>
%%% @version {@vsn}, {@date} {@time}
%%%
%%% @doc This module cleans up expired and orphaned pubsub
%%% subscriptions.
%%%
%%% <p>To use this module simply include it in the modules section of
%%% your ejabberd config. Only one copy of this module is loaded, no
%%% matter how many vhosts have it configured to run.</p>
%%%
%%% <p>There are two configuration options that can be set:</p>
%%% <dl>
%%%   <dt>{notify_time, Seconds}</dt>
%%%   <dd>Send an pending subscription expiry warning `Seconds' before
%%%   expiration</dd>
%%%   <dt>{reap_orphans, bool()}</dt>
%%%   <dd>When true, the state and subscription databases are scanned
%%%   at startup and any orphans found will be cleaned. Note that
%%%   there are not normally orphans, so this should normally be
%%%   false.</dd>
%%% </dl>
%%%
%%% @reference <a
%%% href="http://xmpp.org/extensions/xep-0060.html#impl-leases">XEP-0060
%%% Section 12.18</a>
%%% @reference <a
%%% href="http://xmpp.org/extensions/xep-0060.html#example-205">XEP-0060
%%% example 205</a>.
%%%
%%% @end
%%% ====================================================================

-module(mod_pubsub_reaper).
-author('bjc@kublai.com').

-behavior(gen_server).

-include_lib("stdlib/include/qlc.hrl").

-include("ejabberd.hrl").
-include("jlib.hrl").
-include("pubsub.hrl").

%% API
-export([start/2, start_link/1, stop/1, enqueue/3, timers/0, reap/2]).

%% Callbacks
-export([init/1, terminate/2, code_change/3, handle_call/3,
         handle_cast/2, handle_info/2, notify_pending_expire/2,
         reap_subscription/2]).

-define(SUPERVISOR, ejabberd_sup).

%% Send an event five minutes before the lease expires.
-define(DEFAULT_NOTIFY_TIME, 300).

%% Do not scan for orphans on startup.
-define(DEFAULT_REAP_ORPHANS, false).

-record(state, {options = [], subtimers}).
-record(enqueue, {expire_time, subscription, state}).
-record(timers, {}).
-record(notify_pending, {subid, stateid}).
-record(reap, {subid, stateid}).

%%====================================================================
%% API
%%====================================================================
%% @spec (Host, Opts) -> ok | {error, Reason}
%%       Host   = string()
%%       Opts   = [{term(), term()}]
%%       Reason = term()
%% @doc Starts this module with `Opts' and attaches it to the
%% {@link ejabberd_sup. ejabberd supervisor}. Note that `Host' is
%% currently ignored and only one process is ever spawned for this
%% module.
start(_Host, Opts) ->
    ?INFO_MSG("Starting ~p", [?MODULE]),
    Reaper = {?MODULE, {?MODULE, start_link, [Opts]},
              transient, brutal_kill, worker, [?MODULE]},
    case supervisor:start_child(?SUPERVISOR, Reaper) of
        {ok, _}                       -> ok;
        {error, {already_started, _}} -> ok;
        Other                         -> Other
    end.

%% @spec (Opts) -> {ok, Pid} | ignore | {error, Error}
%%       Opts = [{term(), term()}]
%% @doc Start this module linked to the current process.
start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Opts], []).

%% @spec (Host) -> ok | {error, Reason}
%%       Host = string()
%% @doc Stops this module. Note that `Host' is currently ignored, and
%% stopping this module stops it on all hosts. In addition, regardless
%% of the return value of this function, the module is always stopped.
stop(_Host) ->
    ?INFO_MSG("Stopping ~p", [?MODULE]),
    supervisor:terminate_child(?SUPERVISOR, ?MODULE),
    supervisor:delete_child(?SUPERVISOR, ?MODULE).


%% @spec (When, SubID, StateID) -> ok
%%       When = {MegaSecs, Secs, MicrSecs}
%%       SubID = pubSubSubscriptionId()
%%       StateID = pubsubStateId()
%% @doc Enqueue a cleanup of `SubID' and `StateID' at `When'.
enqueue(When, SubID, StateID) ->
    gen_server:cast(?MODULE, #enqueue{expire_time  = When,
                                      subscription = SubID,
                                      state        = StateID}).

%% @spec () -> dict()
%% @doc Return all active expiration timers keyed by subscription id.
timers() ->
    gen_server:call(?MODULE, #timers{}).

%% @spec (SubID, StateID) -> ok
%%       SubID = pubSubSubscriptionId()
%%       StateID = pubsubStateId()
%% @doc Kill the subscription referenced by `SubID' and `StateID'
%% immediately, sending unsubscribe notifications.
reap(SubID, StateID) ->
    ?MODULE ! #reap{subid = SubID, stateid = StateID},
    ok.

%%====================================================================
%% Callbacks
%%====================================================================
%% @private
init([Options]) ->
    ReapOrphans = find_opt(reap_orphans, ?DEFAULT_REAP_ORPHANS, Options),
    {atomic, ok} = mnesia:transaction(fun scan_state/1,
                                      [ReapOrphans]),
    {ok, #state{options = Options, subtimers = dict:new()}}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @private
handle_call(#timers{}, _From, State) ->
    {reply, State#state.subtimers, State}.

%% @private
handle_cast(#enqueue{expire_time  = When,
                     subscription = SubID,
                     state        = StateID},
            State) ->
    NotifyTime = find_opt(notify_time, ?DEFAULT_NOTIFY_TIME,
                          State#state.options) * 1000,
    case catch timer:now_diff(When, now()) of
	{'EXIT', _Reason} ->
            ?WARNING_MSG("Reaping bogus subscription expiry for ~p: ~p",
                         [SubID, When]),
            reap_immediately(State, StateID, SubID),
            {noreply, State};
        I when (I div 1000) >= NotifyTime ->
            enqueue_notify_pending(I div 1000, NotifyTime, State,
                                   StateID, SubID, When);
        I when I >= 0 ->
            enqueue_reap(I div 1000, State, StateID, SubID, When);
        _ ->
            reap_immediately(State, StateID, SubID)
    end.

%% @private
handle_info(#notify_pending{subid = SubID, stateid = StateID}, State) ->
    notify_pending_expire(SubID, StateID),
    {noreply, State};
handle_info(#reap{subid = SubID, stateid = StateID}, State) ->
    reap_subscription(SubID, StateID),
    Timers = dict:erase(SubID, State#state.subtimers),
    {noreply, State#state{subtimers = Timers}}.

%% @private
notify_pending_expire(SubID, StateID) ->
    ?DEBUG("Notifying pending reap: ~p, state: ~p", [SubID, StateID]),
    {atomic, RC} = mnesia:transaction(fun get_sub_state_node/2,
                                      [SubID, StateID]),
    case RC of
        {value, Sub, State, Node} ->
            case find_opt(expire, Sub#pubsub_subscription.options) of
                false ->
                    ok;
                When ->
                    Subscription = find_subid_in_subs(SubID,
                                                      State#pubsub_state.subscriptions),
                    {Service, NodeID} = Node#pubsub_node.nodeid,
                    {ToJID, _} = StateID,
                    FromJID = service_jid(Service),
                    Stanza = expiry_event(When, ToJID, NodeID,
                                          SubID, Subscription),
                    ejabberd_router:route(FromJID, jlib:make_jid(ToJID),
                                          Stanza),
                    enqueue(When, SubID, StateID)
            end;
        Other ->
            Other
    end.

%% @private
reap_subscription(SubID, StateID) ->
    ?DEBUG("Reaping subscription: ~p, state: ~p", [SubID, StateID]),
    {atomic, ok} = mnesia:transaction(fun reap_subscription_helper/2,
                                      [SubID, StateID]),
    ok.

reap_subscription_helper(SubID, StateID) ->
    case get_sub_state_node(SubID, StateID) of
        {value, Sub, State, Node} ->
            Now = now(),
            case find_opt(expire, Sub#pubsub_subscription.options) of
                When when When < Now ->
                    ok = update_pubsub_state(SubID, State),
                    ok = delete_subscription(Sub),
                    notify_expired_subscription(Sub, State, Node);
                _ ->
                    ok
            end;
        false ->
            ok
    end.
%%====================================================================
%% Internal functions
%%====================================================================
scan_state(true) ->
    ?INFO_MSG("Scanning pubsub state table for orphaned subscriptions.", []),
    reap_orphans(),
    scan_state(false);
scan_state(false) ->
    ?INFO_MSG("Scanning pubsub state table for expiring subscriptions.", []),
    watch_expires().

%% @doc Find all subs in pubsub_state without entries in the state
%% table and delete them.
reap_orphans() ->
    StateQ = qlc:q([State#pubsub_state.subscriptions || State <- mnesia:table(pubsub_state)]),
    StateSubs = [SubID || {_, SubID} <- lists:flatten(qlc:e(StateQ))],
    SubsQ = qlc:q([Sub#pubsub_subscription.subid || Sub <- mnesia:table(pubsub_subscription)]),

    S = qlc:e(SubsQ) -- StateSubs,
    ?INFO_MSG("Found ~p orphans: ~p", [length(S), S]),
    lists:foreach(fun (SubID) ->
                          mnesia:delete({pubsub_subscription, SubID})
                  end, S).

watch_expires() ->
    Q = qlc:q([{State, Sub} || State <- mnesia:table(pubsub_state),
                               Sub   <- mnesia:table(pubsub_subscription),
                               subid_in_subs(Sub#pubsub_subscription.subid,
                                             State#pubsub_state.subscriptions),
                               has_expire(Sub#pubsub_subscription.options)]),
    Rs = qlc:e(Q),

    ?DEBUG("Found ~p subscriptions to watch.", [length(Rs)]),
    lists:foreach(fun ({#pubsub_state{stateid = StateID},
                        #pubsub_subscription{subid = SubID,
                                             options = Options}}) ->
                          When = find_opt(expire, Options),
                          enqueue(When, SubID, StateID)
                  end, Rs).

has_expire(Options) ->
    lists:any(fun ({expire, _}) -> true;
                  (_)           -> false
              end, Options).

%% @spec (SubID, Timer, Timers) -> NewTimers
%% @doc Return a new timer dictionary with `Timer' in `SubID'.
add_timer(SubID, Timer, Timers) ->
    case dict:find(SubID, Timers) of
        {ok, OldTimer} -> timer:cancel(OldTimer);
        _              -> ok
    end,
    dict:store(SubID, Timer, Timers).

enqueue_notify_pending(I, NotifyTime, State, StateID, SubID, When) ->
    After = I - NotifyTime,
    ?DEBUG("Enqueuing pending subscription warning in ~pms"
           "~nSubID: ~p, StateID: ~p, When: ~p",
           [After, SubID, StateID, When]),
    Timer = timer:send_after(After, #notify_pending{subid = SubID,
                                                    stateid = StateID}),
    Timers = add_timer(SubID, Timer, State#state.subtimers),
    {noreply, State#state{subtimers = Timers}}.

enqueue_reap(I, State, StateID, SubID, When) ->
    ?DEBUG("Enqueuing pending subscription cleanup in ~pms"
           "~nSubID: ~p, StateID: ~p, When: ~p",
           [I, SubID, StateID, When]),
    Timer = timer:send_after(I, #reap{subid = SubID, stateid = StateID}),
    Timers = add_timer(SubID, Timer, State#state.subtimers),
    {noreply, State#state{subtimers = Timers}}.

reap_immediately(State, StateID, SubID) ->
    ?DEBUG("Reaping subscription immediately~nSubID: ~p, StateID: ~p",
           [SubID, StateID]),
    ok = reap_subscription(SubID, StateID),
    {noreply, State}.

update_pubsub_state(SubID,
                    #pubsub_state{affiliation = Affiliation,
                                  subscriptions = Subs} = State) ->
    Fun = fun ({_, SubID2}) -> SubID =/= SubID2;
              (_)           -> true
          end,
    case {Affiliation, lists:filter(Fun, Subs)} of
        {none, []} ->
            ok = mnesia:delete_object(State);
        {_, NewSubs} ->
            NewState = State#pubsub_state{subscriptions = NewSubs},
            ok = mnesia:write(NewState)
    end.

delete_subscription(Sub) ->
    ok = mnesia:delete_object(Sub).

notify_expired_subscription(#pubsub_subscription{subid = SubID},
                            #pubsub_state{stateid = {ToJID, _NodeID}},
                            #pubsub_node{nodeid = {Service, Node}}) ->
    FromJID = service_jid(Service),
    Stanza = unsubscribed_event(ToJID, Node, SubID),
    ejabberd_router:route(FromJID, jlib:make_jid(ToJID), Stanza).

subid_in_subs(SubID, Subs) ->
    find_subid_in_subs(SubID, Subs) =/= false.

get_sub_state_node(SubID, {_JID, NodeID} = StateID) ->
    case find_subscription(SubID) of
        {value, Sub} ->
            case find_state(StateID) of
                {value, State} ->
                    case find_node(NodeID) of
                        {value, Node} ->
                            {value, Sub, State, Node};
                        _ ->
                            false
                    end;
                _ ->
                    false
            end;
        _ ->
            false
    end.

find_subscription(SubID) ->
    case catch mnesia:read(pubsub_subscription, SubID) of
        [Record] -> {value, Record};
        []       -> false;
        Error    -> Error
    end.

find_node(NodeID) ->
    case catch mnesia:index_read(pubsub_node, NodeID, #pubsub_node.id) of
        [Record] -> {value, Record};
        []       -> false;
        Error    -> Error
    end.

find_state(StateID) ->
    case catch mnesia:read(pubsub_state, StateID) of
        [Record] -> {value, Record};
        []       -> false;
        Error    -> Error
    end.

find_opt(Option, Options) -> find_opt(Option, false, Options).

find_opt(_,      Default, [])                -> Default;
find_opt(Option, _,       [{Option, V} | _]) -> V;
find_opt(Option, Default, [_ | T])           -> find_opt(Option, Default, T).

find_subid_in_subs(_,     [])                 -> false;
find_subid_in_subs(SubID, [{Sub, SubID} | _]) -> Sub;
find_subid_in_subs(SubID, [_            | T]) -> find_subid_in_subs(SubID, T).

%% @spec (Host) -> jid()
%%	 Host = host()
%% @doc Generate pubsub service JID.
service_jid(Host) ->
    case Host of
        {U, S, _} -> {jid, U, S, "", U, S, ""};
        _         -> {jid, "", Host, "", "", Host, ""}
    end.

%% @spec (When, JID, Node, SubID, Subscription) -> stanza()
%%       When         = {MegaSecs, Secs, MicroSecs}
%%       JID          = jid()
%%       Node         = pubsubNode()
%%       SubID        = string()
%%       Subscription = atom()
%% @doc Return an xml element tuple representing a pending expiry
%% event for `JID' with `SubID' on `Host'.
expiry_event(When, JID, Node, SubID, Subscription) ->
    Attrs = [{"expiry",       jlib:now_to_utc_string(When)},
             {"jid",          jlib:jid_to_string(JID)},
             {"node",         mod_pubsub:node_to_string(Node)},
             {"subid",        SubID},
             {"subscription", atom_to_list(Subscription)}],
    event_stanza([{xmlelement, "subscription", Attrs, []}]).

%% @spec (JID, Node, SubID) -> xmlelement()
%%       JID   = jid()
%%       Node  = pubsubNode()
%%       SubID = string()
%% @doc Return an xml element tuple representing an unsubscribed event
%% for `JID' with `SubID' on `Host'.
unsubscribed_event(JID, Node, SubID) ->
    Attrs = [{"jid",          jlib:jid_to_string(JID)},
             {"node",         mod_pubsub:node_to_string(Node)},
             {"subid",        SubID},
             {"subscription", "unsubscribed"}],
    event_stanza([{xmlelement, "subscription", Attrs, []}]).

event_stanza(Els) ->
    {xmlelement, "message", [{"type", "headline"}],
     [{xmlelement, "event", [{"xmlns", ?NS_PUBSUB_EVENT}], Els}]}.
