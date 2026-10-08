%%% -*- erlang -*-
-module(quic_send_ready_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    connection_credit_emits_exact_send_ready/1,
    stream_credit_emits_exact_send_ready/1,
    combined_credit_emits_only_admissible_send_ready/1,
    readiness_registry_transitions_are_exact/1,
    send_larger_than_flow_window_progresses/1,
    oversized_send_is_atomic/1,
    oversized_send_precedes_flow_control/1,
    queued_send_blocks_without_crashing/1,
    stopped_server_name_is_reusable/1,
    stale_registry_death_preserves_replacement/1
]).

all() ->
    [
        connection_credit_emits_exact_send_ready,
        stream_credit_emits_exact_send_ready,
        combined_credit_emits_only_admissible_send_ready,
        readiness_registry_transitions_are_exact,
        send_larger_than_flow_window_progresses,
        oversized_send_is_atomic,
        oversized_send_precedes_flow_control,
        queued_send_blocks_without_crashing,
        stopped_server_name_is_reusable,
        stale_registry_death_preserves_replacement
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

connection_credit_emits_exact_send_ready(_Config) ->
    assert_flow_credit_wake(
        #{
            max_data => 64,
            max_stream_data_bidi_local => 1024,
            max_stream_data_bidi_remote => 1024
        }
    ).

stream_credit_emits_exact_send_ready(_Config) ->
    assert_flow_credit_wake(
        #{
            max_data => 1024,
            max_stream_data_bidi_local => 64,
            max_stream_data_bidi_remote => 64
        }
    ).

combined_credit_emits_only_admissible_send_ready(_Config) ->
    %% The peer advances connection and stream credit independently. A wake is
    %% correct only after the current state satisfies both; retrying immediately
    %% after the edge must therefore succeed, never re-refuse.
    assert_flow_credit_wake(
        #{
            max_data => 64,
            max_stream_data_bidi_local => 64,
            max_stream_data_bidi_remote => 64
        }
    ).

assert_flow_credit_wake(ServerOpts) ->
    {ok, Echo} = quic_test_echo_server:start(ServerOpts),
    Port = maps:get(port, Echo),
    try
        {ok, Conn} = connect(Port),
        {ok, Sid} = quic:open_stream(Conn),
        ok = quic:send_data(Conn, Sid, binary:copy(<<1>>, 64), false),
        ?assertMatch(
            {error, {flow_control_blocked, _}},
            quic:send_data(Conn, Sid, <<2>>, false)
        ),
        receive
            {quic, Conn, {send_ready, Sid}} -> ok
        after 5000 ->
            ct:fail(no_send_ready_after_flow_credit)
        end,
        %% This is the contract: send_ready means the exact refused size is
        %% admissible in current serialized state, not merely that *some*
        %% unrelated transport progress occurred.
        ok = quic:send_data(Conn, Sid, <<2>>, true),
        quic:close(Conn, normal),
        ok
    after
        quic_test_echo_server:stop(Echo)
    end.

readiness_registry_transitions_are_exact(_Config) ->
    Results = quic_connection:test_send_ready_transitions(),
    ?assertEqual(
        #{
            connection_unready => true,
            connection_ready => true,
            reregistered_unready => true,
            reregistered_ready => true,
            other_stream_no_wake => true,
            stream_ready => true,
            queue_unready => true,
            queue_ready => true,
            dequeue_wake => true,
            trim_wake => true,
            closed_no_wake => true,
            terminal_clear => true
        },
        Results
    ).

send_larger_than_flow_window_progresses(_Config) ->
    Window = 64,
    Payload = binary:copy(<<16#5a>>, 4096),
    {ok, Echo} = quic_test_echo_server:start(
        #{
            max_data => Window,
            max_stream_data_bidi_local => Window,
            max_stream_data_bidi_remote => Window
        }
    ),
    Port = maps:get(port, Echo),
    try
        {ok, Conn} = connect(Port),
        {ok, Sid} = quic:open_stream(Conn),
        %% The transport owns fragmentation across changing flow windows.  The
        %% application submits its frame once and receives it exactly once.
        ok = quic:send_data(Conn, Sid, Payload, true),
        ?assertEqual(Payload, receive_stream(Conn, Sid, <<>>)),
        quic:close(Conn, normal),
        ok
    after
        quic_test_echo_server:stop(Echo)
    end.

receive_stream(Conn, Sid, Acc) ->
    receive
        {quic, Conn, {stream_data, Sid, Data, true}} ->
            <<Acc/binary, Data/binary>>;
        {quic, Conn, {stream_data, Sid, Data, false}} ->
            receive_stream(Conn, Sid, <<Acc/binary, Data/binary>>)
    after 5000 ->
        ct:fail({large_send_stalled, byte_size(Acc)})
    end.

oversized_send_is_atomic(_Config) ->
    Window = 32 * 1024 * 1024,
    {ok, Echo} = quic_test_echo_server:start(
        #{
            max_data => Window,
            max_stream_data_bidi_local => Window,
            max_stream_data_bidi_remote => Window
        }
    ),
    Port = maps:get(port, Echo),
    try
        {ok, Conn} = connect(Port),
        {ok, Sid} = quic:open_stream(Conn),
        %% One byte beyond the existing transport queue owner. The call must
        %% refuse before emitting any prefix; previously chunking could put UDP
        %% packets on the wire and then roll its state back with this error.
        Oversized = binary:copy(<<16#aa>>, 16 * 1024 * 1024 + 1),
        ?assertEqual(
            {error, send_too_large},
            quic:send_data(Conn, Sid, Oversized, false)
        ),
        receive
            {quic, Conn, {stream_data, Sid, Prefix, _}} ->
                ct:fail({oversized_prefix_escaped, byte_size(Prefix)});
            {quic, Conn, {send_ready, Sid}} ->
                ct:fail(oversized_retry_was_falsely_ready)
        after 200 ->
            ok
        end,
        %% A permanent size refusal registers no wake. A later valid call is
        %% delivered exactly once on the unpoisoned stream.
        ok = quic:send_data(Conn, Sid, <<"ok">>, true),
        receive
            {quic, Conn, {stream_data, Sid, <<"ok">>, true}} -> ok
        after 5000 ->
            ct:fail(valid_send_not_delivered_after_oversized_refusal)
        end,
        receive
            {quic, Conn, {stream_data, Sid, Duplicate, _}} ->
                ct:fail({duplicate_after_atomic_refusal, Duplicate})
        after 100 ->
            ok
        end,
        quic:close(Conn, normal),
        ok
    after
        quic_test_echo_server:stop(Echo)
    end.

oversized_send_precedes_flow_control(_Config) ->
    %% Size is a permanent property of the request. Even with only 64 bytes of
    %% current flow credit, an impossible request must not masquerade as a
    %% transient flow refusal and register a wake that can never make it fit.
    {ok, Echo} = quic_test_echo_server:start(
        #{
            max_data => 64,
            max_stream_data_bidi_local => 64,
            max_stream_data_bidi_remote => 64
        }
    ),
    Port = maps:get(port, Echo),
    try
        {ok, Conn} = connect(Port),
        {ok, Sid} = quic:open_stream(Conn),
        Oversized = binary:copy(<<16#bb>>, 16 * 1024 * 1024 + 1),
        ?assertEqual(
            {error, send_too_large},
            quic:send_data(Conn, Sid, Oversized, false)
        ),
        receive
            {quic, Conn, {send_ready, Sid}} ->
                ct:fail(permanent_oversize_registered_for_wake)
        after 100 ->
            ok
        end,
        quic:close(Conn, normal),
        ok
    after
        quic_test_echo_server:stop(Echo)
    end.

queued_send_blocks_without_crashing(_Config) ->
    Parent = self(),
    Handler = fun(Server, _) ->
        ok = quic:set_owner_sync(Server, Parent),
        Parent ! {server_connection, Server},
        {ok, Parent}
    end,
    {ok, Echo} = quic_test_echo_server:start(
        #{
            max_data => 1024,
            max_stream_data_bidi_local => 64,
            max_stream_data_bidi_remote => 64,
            connection_handler => Handler
        }
    ),
    try
        {ok, Conn} = connect(maps:get(port, Echo)),
        Server =
            receive
                {server_connection, S} -> S
            after 5000 ->
                ct:fail(no_server_connection)
            end,
        %% Hold the real peer before it can grant credit. The accepted send
        %% reserves 128 bytes while only 64 can leave the existing queue.
        ok = sys:suspend(Server),
        try
            {ok, Sid} = quic:open_stream(Conn),
            Payload = binary:copy(<<1>>, 128),
            ok = quic:send_data(Conn, Sid, Payload, false),
            ?assertEqual(
                {error, {flow_control_blocked, {stream, Sid}}},
                quic:send_data(Conn, Sid, <<2>>, false)
            ),
            ?assert(is_process_alive(Conn)),
            ok = sys:resume(Server),
            receive
                {quic, Conn, {send_ready, Sid}} -> ok
            after 5000 ->
                ct:fail(no_send_ready_after_queued_send)
            end,
            %% Only the refused byte is submitted again. The previously
            %% accepted payload drains exactly once through the transport.
            ok = quic:send_data(Conn, Sid, <<2>>, true),
            ?assertEqual(<<Payload/binary, 2>>, receive_stream(Server, Sid, <<>>))
        after
            catch sys:resume(Server),
            quic:close(Conn, normal)
        end
    after
        quic_test_echo_server:stop(Echo)
    end.

stopped_server_name_is_reusable(_Config) ->
    {ok, Echo} = quic_test_echo_server:start(),
    Name = maps:get(name, Echo),
    {ok, #{pid := Old, opts := Opts}} = quic_server_registry:lookup(Name),
    Registry = whereis(quic_server_registry),
    Parent = self(),
    Ref = make_ref(),
    %% Forward calls to the real registry while holding its mailbox. This
    %% makes the historical gap after stop deterministic without sleeps.
    Proxy = spawn(fun() -> registry_proxy(Registry, Parent, Ref) end),
    true = unregister(quic_server_registry),
    true = register(quic_server_registry, Proxy),
    ok = sys:suspend(Registry),
    {Worker, Monitor} = spawn_monitor(fun() ->
        ok = quic:stop_server(Name),
        Parent ! {stop_returned, Ref, quic_server_registry:lookup(Name)}
    end),
    try
        Observed =
            receive
                {registry_call, Ref} ->
                    ok = sys:resume(Registry),
                    receive
                        {stop_returned, Ref, Result} -> Result
                    after 5000 -> ct:fail(stop_did_not_complete)
                    end;
                {stop_returned, Ref, Result} ->
                    Result
            after 5000 -> ct:fail(stop_did_not_reach_registry)
            end,
        Summary =
            case Observed of
                {ok, #{pid := Owner}} -> {ok, Owner};
                Other -> Other
            end,
        ?assertEqual({error, not_found}, Summary),
        true = unregister(quic_server_registry),
        true = register(quic_server_registry, Registry),
        {ok, New} = quic:start_server(Name, 0, Opts),
        ?assertNotEqual(Old, New),
        ?assertMatch({ok, #{pid := New}}, quic_server_registry:lookup(Name)),
        ok = quic:stop_server(Name),
        ?assertEqual({error, not_found}, quic_server_registry:lookup(Name))
    after
        catch sys:resume(Registry),
        case whereis(quic_server_registry) of
            Proxy ->
                true = unregister(quic_server_registry),
                true = register(quic_server_registry, Registry);
            Registry ->
                ok
        end,
        exit(Proxy, kill),
        exit(Worker, kill),
        receive
            {'DOWN', Monitor, process, Worker, _} -> ok
        after 5000 -> ct:fail(stop_worker_not_closed)
        end,
        quic_test_echo_server:stop(Echo)
    end.

registry_proxy(Registry, Parent, Ref) ->
    receive
        {'$gen_call', _, _} = Message ->
            Parent ! {registry_call, Ref},
            Registry ! Message,
            registry_proxy(Registry, Parent, Ref)
    end.

stale_registry_death_preserves_replacement(_Config) ->
    Name = stale_registry_owner,
    Old = spawn(fun() ->
        receive
            stop -> ok
        end
    end),
    New = spawn(fun() ->
        receive
            stop -> ok
        end
    end),
    Registry = whereis(quic_server_registry),
    try
        ok = quic_server_registry:register(Name, Old, 1234, #{}),
        {state, Monitors} = sys:get_state(Registry),
        [OldRef] = [R || {R, N} <- maps:to_list(Monitors), N =:= Name],
        ok = quic_server_registry:register(Name, New, 1234, #{}),
        exit(Old, kill),
        %% A delayed genuine reference from the previous owner is harmless.
        Registry ! {'DOWN', OldRef, process, Old, killed},
        _ = sys:get_state(Registry),
        ?assertMatch({ok, #{pid := New}}, quic_server_registry:lookup(Name)),
        ok = quic_server_registry:unregister_stopped(Name),
        ?assertMatch({ok, #{pid := New}}, quic_server_registry:lookup(Name)),
        NewMonitor = erlang:monitor(process, New),
        exit(New, kill),
        receive
            {'DOWN', NewMonitor, process, New, killed} -> ok
        after 5000 -> ct:fail(replacement_not_stopped)
        end,
        ok = quic_server_registry:unregister_stopped(Name),
        ?assertEqual({error, not_found}, quic_server_registry:lookup(Name))
    after
        exit(Old, kill),
        exit(New, kill),
        quic_server_registry:unregister(Name)
    end.

connect(Port) ->
    {ok, Conn} = quic:connect(
        "127.0.0.1",
        Port,
        #{verify => false, alpn => [<<"echo">>]},
        self()
    ),
    receive
        {quic, Conn, {connected, _}} -> {ok, Conn}
    after 5000 ->
        ct:fail(no_connection)
    end.
