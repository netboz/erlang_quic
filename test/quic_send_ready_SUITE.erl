%%% -*- erlang -*-
-module(quic_send_ready_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         connection_credit_emits_exact_send_ready/1,
         stream_credit_emits_exact_send_ready/1,
         combined_credit_emits_only_admissible_send_ready/1,
         readiness_registry_transitions_are_exact/1,
         oversized_send_is_atomic/1,
         oversized_send_precedes_flow_control/1]).

all() -> [connection_credit_emits_exact_send_ready,
          stream_credit_emits_exact_send_ready,
          combined_credit_emits_only_admissible_send_ready,
          readiness_registry_transitions_are_exact,
          oversized_send_is_atomic,
          oversized_send_precedes_flow_control].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

connection_credit_emits_exact_send_ready(_Config) ->
    assert_flow_credit_wake(
      #{max_data => 64,
        max_stream_data_bidi_local => 1024,
        max_stream_data_bidi_remote => 1024}).

stream_credit_emits_exact_send_ready(_Config) ->
    assert_flow_credit_wake(
      #{max_data => 1024,
        max_stream_data_bidi_local => 64,
        max_stream_data_bidi_remote => 64}).

combined_credit_emits_only_admissible_send_ready(_Config) ->
    %% The peer advances connection and stream credit independently. A wake is
    %% correct only after the current state satisfies both; retrying immediately
    %% after the edge must therefore succeed, never re-refuse.
    assert_flow_credit_wake(
      #{max_data => 64,
        max_stream_data_bidi_local => 64,
        max_stream_data_bidi_remote => 64}).

assert_flow_credit_wake(ServerOpts) ->
    {ok, Echo} = quic_test_echo_server:start(ServerOpts),
    Port = maps:get(port, Echo),
    try
        {ok, Conn} = connect(Port),
        {ok, Sid} = quic:open_stream(Conn),
        ok = quic:send_data(Conn, Sid, binary:copy(<<1>>, 64), false),
        ?assertMatch(
           {error, {flow_control_blocked, _}},
           quic:send_data(Conn, Sid, <<2>>, false)),
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
       #{connection_unready => true,
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
         terminal_clear => true},
       Results).

oversized_send_is_atomic(_Config) ->
    Window = 32 * 1024 * 1024,
    {ok, Echo} = quic_test_echo_server:start(
                   #{max_data => Window,
                     max_stream_data_bidi_local => Window,
                     max_stream_data_bidi_remote => Window}),
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
           quic:send_data(Conn, Sid, Oversized, false)),
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
                   #{max_data => 64,
                     max_stream_data_bidi_local => 64,
                     max_stream_data_bidi_remote => 64}),
    Port = maps:get(port, Echo),
    try
        {ok, Conn} = connect(Port),
        {ok, Sid} = quic:open_stream(Conn),
        Oversized = binary:copy(<<16#bb>>, 16 * 1024 * 1024 + 1),
        ?assertEqual(
           {error, send_too_large},
           quic:send_data(Conn, Sid, Oversized, false)),
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

connect(Port) ->
    {ok, Conn} = quic:connect(
                   "127.0.0.1", Port,
                   #{verify => false, alpn => [<<"echo">>]}, self()),
    receive
        {quic, Conn, {connected, _}} -> {ok, Conn}
    after 5000 ->
        ct:fail(no_connection)
    end.
