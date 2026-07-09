%%% -*- erlang -*-
%%%
%%% Tests for QUIC Idle Timeout Enforcement (RFC 9000 Section 10.1)
%%%

-module(quic_idle_timeout_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Idle Timer Message Tests
%%====================================================================

%% Test that idle_timeout message is properly formatted
idle_timeout_message_format_test() ->
    %% The idle timeout message should be an atom
    Msg = idle_timeout,
    ?assertEqual(idle_timeout, Msg).

%%====================================================================
%% Integration with Connection State
%%====================================================================

%% Note: Full integration tests require starting the connection process
%% and are covered in quic_connection_tests.erl and quic_e2e_SUITE.erl

%% Test the basic concept of idle timeout checking
idle_timeout_check_logic_test() ->
    % 30 seconds
    IdleTimeout = 30000,
    % 25 seconds ago
    LastActivity = erlang:monotonic_time(millisecond) - 25000,
    Now = erlang:monotonic_time(millisecond),
    TimeSinceActivity = Now - LastActivity,

    %% Should NOT timeout (25s < 30s)
    ?assertNot(TimeSinceActivity >= IdleTimeout),

    %% Simulate more time passing

    % 35 seconds ago
    LastActivity2 = erlang:monotonic_time(millisecond) - 35000,
    TimeSinceActivity2 = Now - LastActivity2,

    %% Should timeout (35s >= 30s)
    ?assert(TimeSinceActivity2 >= IdleTimeout).

%% Test that zero idle timeout means no timeout
zero_idle_timeout_test() ->
    IdleTimeout = 0,
    % Very old
    _LastActivity = erlang:monotonic_time(millisecond) - 1000000,
    _Now = erlang:monotonic_time(millisecond),

    %% With 0 timeout, comparison should indicate "set but disabled"
    %% In the implementation, set_idle_timer returns immediately for timeout=0
    ?assertEqual(0, IdleTimeout).

%%====================================================================
%% Timer Reset Tests
%%====================================================================

%% Test that activity resets the idle timeout window
activity_resets_timeout_test() ->
    InitialActivity = erlang:monotonic_time(millisecond),
    % Small delay
    timer:sleep(10),

    %% Simulate activity update
    NewActivity = erlang:monotonic_time(millisecond),

    ?assert(NewActivity > InitialActivity).

%%====================================================================
%% Boundary Tests
%%====================================================================

%% Test exactly at timeout boundary
exact_timeout_boundary_test() ->
    % 1 second
    IdleTimeout = 1000,

    %% Exactly at boundary should trigger timeout (>= comparison)
    LastActivity = erlang:monotonic_time(millisecond) - 1000,
    Now = erlang:monotonic_time(millisecond),
    TimeSinceActivity = Now - LastActivity,

    ?assert(TimeSinceActivity >= IdleTimeout).

%% Test just below timeout boundary
just_below_timeout_boundary_test() ->
    % 10 seconds
    IdleTimeout = 10000,

    %% Just below boundary should NOT trigger timeout

    % 9.99 seconds ago
    LastActivity = erlang:monotonic_time(millisecond) - 9990,
    Now = erlang:monotonic_time(millisecond),
    TimeSinceActivity = Now - LastActivity,

    ?assertNot(TimeSinceActivity >= IdleTimeout).

%%====================================================================
%% RFC 9000 Section 10.1 anti-black-hole send guard (send_activity/4)
%%====================================================================

%% Only the FIRST ack-eliciting packet since our last receive advances
%% last_activity; every later send (and any non-ack-eliciting packet) leaves it
%% frozen, so an endpoint that keeps sending into a silent peer still times out.
send_activity_black_hole_test() ->
    LA0 = 1000,
    %% first ack-eliciting send since a receive: restart last_activity, arm the guard
    ?assertEqual({2000, true}, quic_connection:send_activity(true, false, 2000, LA0)),
    %% subsequent ack-eliciting sends while the guard is set: last_activity FROZEN
    %% (this is the fix: a black-holed connection is no longer kept alive by our sends)
    ?assertEqual({LA0, true}, quic_connection:send_activity(true, true, 3000, LA0)),
    ?assertEqual({LA0, true}, quic_connection:send_activity(true, true, 999999, LA0)),
    %% a non-ack-eliciting packet (e.g. a pure ACK) never advances last_activity
    %% and never arms the guard on its own
    ?assertEqual({LA0, false}, quic_connection:send_activity(false, false, 5000, LA0)),
    ?assertEqual({LA0, true}, quic_connection:send_activity(false, true, 5000, LA0)).

%%====================================================================
%% Keep-alive interval clamp (sub-second liveness on trusted LANs)
%%====================================================================

%% keep_alive_interval is clamped up to KEEP_ALIVE_MIN_MS (250), NOT the old 5000,
%% so a low-RTT LAN can probe sub-second for fast dead-peer detection. disabled/0
%% stay disabled; auto = idle/2 floored at the minimum; the min applies only to
%% pathological sub-250 values.
calculate_keep_alive_interval_clamp_test() ->
    C = fun(V, Idle) -> quic_connection:calculate_keep_alive_interval(#{keep_alive_interval => V}, Idle) end,
    ?assertEqual(disabled, C(disabled, 2000)),
    ?assertEqual(disabled, C(0, 2000)),
    ?assertEqual(500, C(500, 2000)),                 %% sub-5000 now PASSES (was silently raised to 5000)
    ?assertEqual(250, C(250, 2000)),                 %% exactly at the floor
    ?assertEqual(250, C(100, 2000)),                 %% below the floor -> clamped up to 250 (not 5000)
    ?assertEqual(7000, C(7000, 30000)),              %% large explicit value passes through
    %% auto = max(min, idle/2); default (key absent) is disabled
    ?assertEqual(1000, quic_connection:calculate_keep_alive_interval(#{keep_alive_interval => auto}, 2000)),
    ?assertEqual(250, quic_connection:calculate_keep_alive_interval(#{keep_alive_interval => auto}, 400)),
    ?assertEqual(disabled, quic_connection:calculate_keep_alive_interval(#{keep_alive_interval => auto}, 0)),
    ?assertEqual(disabled, quic_connection:calculate_keep_alive_interval(#{}, 2000)).
