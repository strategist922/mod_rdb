-module(mod_rdb).

-behaviour(gen_mod).
-export([
    start/2,
    stop/1,
    start_link/2,
    reply_received/5
]).

-behaviour(gen_server).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-include("ejabberd.hrl").
-include("jlib.hrl").
-include_lib("stdlib/include/qlc.hrl").

-record(rdb_bot, {rdb_host_jid, prio}).
-record(rdb_doc, {rdb_host_name, jid}).
-record(rdb_route, {rdb_host_name, to}).
-record(state, {host, rdb_host}).

start(Host, Opts) ->
    ?INFO_MSG("mod_rdb: starting module", []),
    mnesia:create_table(rdb_bot, [
        {attributes, record_info(fields, rdb_bot)},
        {ram_copies, [node()]}
    ]),
    mnesia:create_table(rdb_doc, [
        {attributes, record_info(fields, rdb_doc)},
        {ram_copies, [node()]}
    ]),
    mnesia:create_table(rdb_route, [
        {attributes, record_info(fields, rdb_route)},
        {ram_copies, [node()]},
        {type, bag} 
    ]), 
    Proc = gen_mod:get_module_proc(Host, ejabberd_mod_rdb),
    supervisor:start_child(ejabberd_sup, {
        Proc,
        {?MODULE, start_link, [Host, Opts]},
        temporary,
        1000,
        worker,
        [?MODULE]
    }).

stop(Host) ->
    ?INFO_MSG("mod_rdb: stopping module", []),
    Proc = gen_mod:get_module_proc(Host, ejabberd_mod_rdb),
    supervisor:terminate_child(ejabberd_sup, Proc),
    supervisor:delete_child(ejabberd_sup, Proc),
    mnesia:delete_table(rdb_route),
    mnesia:delete_table(rdb_doc),
    mnesia:delete_table(rdb_bot).

start_link(Host, Opts) ->
    Proc = gen_mod:get_module_proc(Host, ejabberd_mod_rdb),
    gen_server:start_link({local, Proc}, ?MODULE, [Host, Opts], []).

reply_received(Host, RDBHost, UserJID, Name, Packet) ->
    Proc = gen_mod:get_module_proc(Host, ejabberd_mod_rdb),
    gen_server:call(Proc, {reply_received, RDBHost, UserJID, Name, Packet}).

init([Host, Opts]) ->
    process_flag(trap_exit, true),
    RDBHost = gen_mod:get_opt_host(Host, Opts, "rdb.@HOST@"),
    ejabberd_router:register_route(RDBHost),
    {ok, #state{host = Host, rdb_host = RDBHost}}.

handle_call({reply_received, RDBHost, UserJID, Name, Packet}, From, State) ->
    {PID, _} = From,
    unregister_route(RDBHost, Name, {pid, PID}),
    {xmlelement, "presence", Attrs, _} = Packet,
    case xml:get_attr_s("type", Attrs) of
    "" ->
        add_user(RDBHost, UserJID, Name);
    _ ->
        ignore
    end,
    DocJID = jlib:make_jid(Name, RDBHost, ""),
    ejabberd_router:route(DocJID, UserJID, Packet),
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({route, From, To, Packet}, State) ->
    ?INFO_MSG("mod_rdb: message received", []),
    #state{
        host = Host,
        rdb_host = RDBHost
    } = State,
    case parse(Host, RDBHost, From, To, Packet) of
    reply ->
        ejabberd_router:route(To, From, Packet);
    noreply ->
        ignore;
    {error, Error} ->
        Reply = jlib:make_error_reply(Packet, Error),
        ejabberd_router:route(To, From, Reply)
    end,
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    ejabberd_router:unregister_route(State#state.rdb_host).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

unregister_route(RDBHost, Name, To) ->
    mnesia:dirty_delete_object(#rdb_route{
        rdb_host_name = {RDBHost, Name},
        to = To
    }).

add_user(RDBHost, UserJID, Name) ->
    register_route(RDBHost, Name, {jid, UserJID}),
    broadcast_presence(
        RDBHost,
        UserJID,
        Name,
        {xmlelement, "presence", [], []}
    ).

parse(Host, RDBHost, From, To, Packet) ->
    {User, _, _} = jlib:jid_tolower(To),
    {xmlelement, Elem, Attrs, _} = Packet,
    case mnesia:dirty_read(rdb_bot, {RDBHost, From}) of
    [_] ->
        case User of
        "" ->
            case Elem of
            "presence" ->
                case xml:get_attr_s("type", Attrs) of
                "" ->
                    Prio = xml:get_attr_s("prio", Attrs),
                    register_bot(RDBHost, From, string:to_integer(Prio));
                "unavailable" ->
                    unregister_bot(RDBHost, From);
                _ ->
                    {error, ?ERR_BAD_REQUEST}
                end;
            _ ->
                {error, ?ERR_BAD_REQUEST}
            end;
        _ ->
            {error, ?ERR_BAD_REQUEST}
        end;
    [] ->
        case User of
        "" ->
                case Elem of
                "presence" ->
                    case xml:get_attr_s("type", Attrs) of
                    "" ->
                        Prio = xml:get_attr_s("prio", Attrs),
                        register_bot(RDBHost, From, string:to_integer(Prio));
                    _ ->
                        {error, ?ERR_BAD_REQUEST}
                    end;
                _ ->
                    {error, ?ERR_BAD_REQUEST}
                end;
        _ ->
            case Elem of
            "presence" ->
                case xml:get_attr_s("type", Attrs) of
                "" ->
                    join_doc(Host, RDBHost, From, User);
                "unavailable" ->
                    leave_doc(RDBHost, From, User);
                _ ->
                    {error, ?ERR_BAD_REQUEST}
                end;
            "message" ->
                broadcast_message(RDBHost, From, User, Packet);
            _ ->
                {error, ?ERR_BAD_REQUEST}
            end
        end
    end.

register_bot(RDBHost, BotJID, Prio) ->
    ?INFO_MSG("mod_rdb: registering bot", []),
    mnesia:dirty_write(#rdb_bot{rdb_host_jid = {RDBHost, BotJID}, prio = Prio}),
    reply.

unregister_bot(RDBHost, BotJID) ->
    ?INFO_MSG("mod_rdb: unregistering bot", []),
    ok = mnesia:dirty_delete(rdb_bot, {RDBHost, BotJID}),
    case mnesia:activity(async_dirty, fun() ->
        Q = qlc:q([X || X <- mnesia:table(rdb_doc),
            X#rdb_doc.jid =:= BotJID]),
        qlc:e(Q)
    end) of
    [] ->
        ignore;
    Docs ->
        lists:foreach(fun(Doc) ->
            case Doc#rdb_doc.rdb_host_name of
            {RDBHost, Name} ->
                case init_doc(RDBHost, Name) of
                {ok, _} ->
                    ignore;
                fail ->
                    deinit_doc(RDBHost, Name)
                end;
            _ ->
                ignore
            end
        end, Docs)
    end,
    reply.

join_doc(Host, RDBHost, UserJID, Name) ->
    ?INFO_MSG("mod_rdb: joining doc", []),
    case init_doc(RDBHost, Name) of
    {ok, BotJID} ->
        DocJID = jlib:make_jid(Name, RDBHost, ""),
        ejabberd_router:route(DocJID, BotJID, {
            xmlelement,
            "presence",
            [{"join", jlib:jid_to_string(UserJID)}],
            []
        }),
        PID = spawn_link(fun() ->
            receive_reply(Host, RDBHost, UserJID, Name, BotJID)
        end),
        register_route(RDBHost, Name, {pid, PID}),
        noreply;
    fail ->
        {error, ?ERR_ITEM_NOT_FOUND}
    end.

receive_reply(Host, RDBHost, UserJID, Name, BotJID) ->
    DocJID = jlib:make_jid(Name, RDBHost, ""),
    receive
    {route, BotJID, DocJID, Packet} ->
        {xmlelement, _, Attrs, _} = Packet,
        Join = xml:get_attr_s("join", Attrs),
        case jlib:string_to_jid(Join) of
        UserJID ->
            reply_received(Host, RDBHost, UserJID, Name, Packet),
            flush();
        _ ->
            receive_reply(Host, RDBHost, UserJID, Name, BotJID)
        end
    end.

flush() ->
    receive
    {route, From, To, Packet} ->
        {xmlelement, Elem, _, _} = Packet,
        case Elem of
        "message" ->
            ejabberd_router:route(From, To, Packet);
        _ ->
            ignore
        end,
        flush()
    after 0 -> 
        void
    end.

register_route(RDBHost, Name, To) ->
    mnesia:dirty_write(#rdb_route{rdb_host_name = {RDBHost, Name}, to = To}).

init_doc(RDBHost, Name) ->
    case mnesia:dirty_read(rdb_doc, {RDBHost, Name}) of
        [] ->
            case select_bot() of
            {ok, BotJID} ->
                mnesia:dirty_write(#rdb_doc{
                    rdb_host_name = {RDBHost, Name},
                    jid = BotJID
                }),
                add_user(RDBHost, BotJID, Name),
                {ok, BotJID};
            fail ->
                fail
            end;
        [Doc] ->
            {ok, Doc#rdb_doc.jid}
    end.

select_bot() ->
    case mnesia:activity(async_dirty, fun() ->
        Q1 = qlc:q([X || X <- mnesia:table(rdb_bot)]),
        Q2 = qlc:sort(Q1, {order, fun(X, Y) ->
            X#rdb_bot.prio =< Y#rdb_bot.prio
        end}),
        qlc:e(Q2)
    end) of
    [H | T] ->
        Prefix = [H | lists:takewhile(fun(X) ->
            H#rdb_bot.prio == X#rdb_bot.prio
        end, T)],
        Length = length(Prefix),
        N = random:uniform(Length),
        Bot = lists:nth(N, Prefix),
        {_, BotJID} = Bot#rdb_bot.rdb_host_jid,
        {ok, BotJID};
    [] ->
        fail
    end.

leave_doc(RDBHost, UserJID, Name) ->
    ?INFO_MSG("mod_rdb: leaving doc", []),
    case is_member_of(RDBHost, UserJID, Name) of
    true ->
        remove_user(RDBHost, UserJID, Name),
        noreply;
    false ->
        {error, ?ERR_FORBIDDEN}
    end.

remove_user(RDBHost, UserJID, Name) ->
    broadcast_presence(RDBHost, UserJID, Name, {
        xmlelement,
        "presence",
        [{"type", "unavailable"}],
        []
    }),
    unregister_route(RDBHost, Name, {jid, UserJID}),
    check_doc(RDBHost, Name).

check_doc(RDBHost, Name) ->
    case mnesia:activity(async_dirty, fun() ->
        Q = qlc:q([X || X <- mnesia:table(rdb_route),
            X#rdb_route.rdb_host_name =:= {RDBHost, Name}]),
        qlc:e(Q)
    end) of
    [] -> 
        deinit_doc(RDBHost, Name);
    Routes ->
        case lists:any(fun(Route) ->
            {_, _, To} = Route,
            case To of
            {jid, _} ->
                true;
            {pid, _} ->
                false
            end
        end, Routes) of
        true ->
            ignore;
        false ->
            deinit_doc(RDBHost, Name)
        end
    end.

deinit_doc(RDBHost, Name) ->
    ?INFO_MSG("mod_rdb: deiniting doc", []),
    mnesia:dirty_delete(rdb_doc, {RDBHost, Name}),
    DocJID = jlib:make_jid(Name, RDBHost, ""),
    broadcast_presence(RDBHost, DocJID, Name, {
        xmlelement,
        "presence",
        [{"type", "unavailable"}],
        []
    }),
    ok = mnesia:dirty_delete(rdb_route, {RDBHost, Name}).

broadcast_presence(RDBHost, From, Name, Packet) ->
    ?INFO_MSG("mod_rdb: broadcasting presence", []),
    DocJID = jlib:make_jid(RDBHost, Name, ""),
    Routes = mnesia:dirty_read(rdb_route, {RDBHost, Name}),
    lists:foreach(fun(Route) ->
        {_, _, To} = Route,
        case To of
        {jid, _JID} -> 
            ignore;
        {pid, PID} ->
            PID ! {route, From, DocJID, Packet}
        end
    end, Routes).

broadcast_message(RDBHost, From, Name, Packet) ->
    ?INFO_MSG("mod_rdb: broadcasting message", []),
    case is_member_of(RDBHost, From, Name) of
    true ->
        broadcast(RDBHost, From, Name, Packet);
    false ->
        {error, ?ERR_FORBIDDEN}
    end.

broadcast(RDBHost, From, Name, Packet) ->
    DocJID = jlib:make_jid(RDBHost, Name, ""),
    Routes = mnesia:dirty_read(rdb_route, {RDBHost, Name}),
    lists:foreach(fun(Route) ->
        {_, _, To} = Route,
        case To of
        {jid, JID} -> 
            ejabberd_router:route(DocJID, JID, Packet);
        {pid, PID} ->
            PID ! {route, From, DocJID, Packet}
        end
    end, Routes).

is_member_of(_RDBHost, _UserJID, _Name) ->
    true.
