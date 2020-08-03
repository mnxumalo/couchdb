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

-module(couch_replicator_scheduler).

-behaviour(gen_server).
-behaviour(config_listener).

-export([
   reschedule/0,
   rep_state/1,
   find_jobs_by_dbname/1,
   find_jobs_by_doc/2,
   job_summary/2,
   jobs/0,
   job/1,
   restart_job/1
]).

%% for status updater process to allow hot code loading
-export([
    stats_updater_loop/1
]).

-include("couch_replicator_scheduler.hrl").
-include("couch_replicator.hrl").
-include_lib("couch_replicator/include/couch_replicator_api_wrap.hrl").
-include_lib("couch/include/couch_db.hrl").

%% types
-type event_type() :: added | started | stopped | {crashed, any()}.
-type event() :: {Type:: event_type(), When :: erlang:timestamp()}.
-type history() :: nonempty_list(event()).

%% definitions
-define(MAX_BACKOFF_EXPONENT, 10).
-define(BACKOFF_INTERVAL_MICROS, 30 * 1000 * 1000).
-define(DEFAULT_HEALTH_THRESHOLD_SEC, 2 * 60).
-define(RELISTEN_DELAY, 5000).
-define(STATS_UPDATE_WAIT, 5000).

-define(DEFAULT_MAX_JOBS, 500).
-define(DEFAULT_MAX_CHURN, 20).
-define(DEFAULT_MAX_HISTORY, 20).
-define(DEFAULT_SCHEDULER_INTERVAL, 60000).


%% public functions

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


-spec add_job(#rep{}) -> ok.
add_job(#rep{} = Rep) when Rep#rep.id /= undefined ->
    case existing_replication(Rep) of
        false ->
            Job = #job{
                id = Rep#rep.id,
                rep = Rep,
                history = [{added, os:timestamp()}]
            },
            gen_server:call(?MODULE, {add_job, Job}, infinity);
        true ->
            ok
    end.


-spec remove_job(job_id()) -> ok.
remove_job(Id) ->
    gen_server:call(?MODULE, {remove_job, Id}, infinity).


-spec reschedule() -> ok.
% Trigger a manual reschedule. Used for testing and/or ops.
reschedule() ->
    gen_server:call(?MODULE, reschedule, infinity).


-spec rep_state(rep_id()) -> #rep{} | nil.
rep_state(RepId) ->
    case (catch ets:lookup_element(?MODULE, RepId, #job.rep)) of
        {'EXIT',{badarg, _}} ->
            nil;
        Rep ->
            Rep
    end.


-spec job_summary(job_id(), non_neg_integer()) -> [_] | nil.
job_summary(JobId, HealthThreshold) ->
    case job_by_id(JobId) of
        {ok, #job{pid = Pid, history = History, rep = Rep}} ->
            ErrorCount = consecutive_crashes(History, HealthThreshold),
            {State, Info} = case {Pid, ErrorCount} of
                {undefined, 0}  ->
                    case History of
                        [{{crashed, Error}, _When} | _] ->
                            {crashing, crash_reason_json(Error)};
                        [_ | _] ->
                            {pending, Rep#rep.stats}
                    end;
                {undefined, ErrorCount} when ErrorCount > 0 ->
                     [{{crashed, Error}, _When} | _] = History,
                     {crashing, crash_reason_json(Error)};
                {Pid, ErrorCount} when is_pid(Pid) ->
                     {running, Rep#rep.stats}
            end,
            [
                {source, iolist_to_binary(ejson_url(Rep#rep.source))},
                {target, iolist_to_binary(ejson_url(Rep#rep.target))},
                {state, State},
                {info, couch_replicator_utils:ejson_state_info(Info)},
                {error_count, ErrorCount},
                {last_updated, last_updated(History)},
                {start_time,
                    couch_replicator_utils:iso8601(Rep#rep.start_time)},
                {source_proxy, job_proxy_url(Rep#rep.source)},
                {target_proxy, job_proxy_url(Rep#rep.target)}
            ];
        {error, not_found} ->
            nil  % Job might have just completed
    end.


job_proxy_url(#httpdb{proxy_url = ProxyUrl}) when is_list(ProxyUrl) ->
    list_to_binary(couch_util:url_strip_password(ProxyUrl));
job_proxy_url(_Endpoint) ->
    null.


-spec find_jobs_by_dbname(binary()) -> list(#rep{}).
find_jobs_by_dbname(DbName) ->
    Rep = #rep{db_name = DbName, _ = '_'},
    MatchSpec = #job{id = '$1', rep = Rep, _ = '_'},
    [RepId || [RepId] <- ets:match(?MODULE, MatchSpec)].


-spec find_jobs_by_doc(binary(), binary()) -> list(#rep{}).
find_jobs_by_doc(DbName, DocId) ->
    Rep =  #rep{db_name = DbName, doc_id = DocId, _ = '_'},
    MatchSpec = #job{id = '$1', rep = Rep, _ = '_'},
    [RepId || [RepId] <- ets:match(?MODULE, MatchSpec)].


-spec restart_job(binary() | list() | rep_id()) ->
    {ok, {[_]}} | {error, not_found}.
restart_job(JobId) ->
    case rep_state(JobId) of
        nil ->
            {error, not_found};
        #rep{} = Rep ->
            ok = remove_job(JobId),
            ok = add_job(Rep),
            job(JobId)
    end.


-spec ejson_url(#httpdb{} | binary()) -> binary().
ejson_url(#httpdb{}=Httpdb) ->
    couch_util:url_strip_password(Httpdb#httpdb.url);
ejson_url(DbName) when is_binary(DbName) ->
    DbName.


-spec job_ejson(#job{}) -> {[_ | _]}.
job_ejson(Job) ->
    Rep = Job#job.rep,
    Source = ejson_url(Rep#rep.source),
    Target = ejson_url(Rep#rep.target),
    History = lists:map(fun({Type, When}) ->
        EventProps  = case Type of
            {crashed, Reason} ->
                [{type, crashed}, {reason, crash_reason_json(Reason)}];
            Type ->
                [{type, Type}]
        end,
        {[{timestamp, couch_replicator_utils:iso8601(When)} | EventProps]}
    end, Job#job.history),
    {BaseID, Ext} = Job#job.id,
    Pid = case Job#job.pid of
        undefined ->
            null;
        P when is_pid(P) ->
            ?l2b(pid_to_list(P))
    end,
    {[
        {id, iolist_to_binary([BaseID, Ext])},
        {pid, Pid},
        {source, iolist_to_binary(Source)},
        {target, iolist_to_binary(Target)},
        {database, Rep#rep.db_name},
        {user, (Rep#rep.user_ctx)#user_ctx.name},
        {doc_id, Rep#rep.doc_id},
        {info, couch_replicator_utils:ejson_state_info(Rep#rep.stats)},
        {history, History},
        {node, node()},
        {start_time, couch_replicator_utils:iso8601(Rep#rep.start_time)}
    ]}.


-spec jobs() -> [[tuple()]].
jobs() ->
    ets:foldl(fun(Job, Acc) -> [job_ejson(Job) | Acc] end, [], ?MODULE).


-spec job(job_id()) -> {ok, {[_ | _]}} | {error, not_found}.
job(JobId) ->
    case job_by_id(JobId) of
        {ok, Job} ->
            {ok, job_ejson(Job)};
        Error ->
            Error
    end.


crash_reason_json({_CrashType, Info}) when is_binary(Info) ->
    Info;
crash_reason_json(Reason) when is_binary(Reason) ->
    Reason;
crash_reason_json(Error) ->
    couch_replicator_utils:rep_error_to_binary(Error).



-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").


backoff_micros_test_() ->
    BaseInterval = ?BACKOFF_INTERVAL_MICROS,
    [?_assertEqual(R * BaseInterval, backoff_micros(N)) || {R, N} <- [
        {1, 1}, {2, 2}, {4, 3}, {8, 4}, {16, 5}, {32, 6}, {64, 7}, {128, 8},
        {256, 9}, {512, 10}, {1024, 11}, {1024, 12}
    ]].


consecutive_crashes_test_() ->
    Threshold = ?DEFAULT_HEALTH_THRESHOLD_SEC,
    [?_assertEqual(R, consecutive_crashes(H, Threshold)) || {R, H} <- [
        {0, []},
        {0, [added()]},
        {0, [stopped()]},
        {0, [crashed()]},
        {1, [crashed(), added()]},
        {1, [crashed(), crashed()]},
        {1, [crashed(), stopped()]},
        {3, [crashed(), crashed(), crashed(), added()]},
        {2, [crashed(), crashed(), stopped()]},
        {1, [crashed(), started(), added()]},
        {2, [crashed(3), started(2), crashed(1), started(0)]},
        {0, [stopped(3), started(2), crashed(1), started(0)]},
        {1, [crashed(3), started(2), stopped(1), started(0)]},
        {0, [crashed(999), started(0)]},
        {1, [crashed(999), started(998), crashed(997), started(0)]}
    ]].


consecutive_crashes_non_default_threshold_test_() ->
    [?_assertEqual(R, consecutive_crashes(H, T)) || {R, H, T} <- [
        {0, [crashed(11), started(0)], 10},
        {1, [crashed(10), started(0)], 10}
    ]].


latest_crash_timestamp_test_() ->
    [?_assertEqual({0, R, 0}, latest_crash_timestamp(H)) || {R, H} <- [
         {0, [added()]},
         {1, [crashed(1)]},
         {3, [crashed(3), started(2), crashed(1), started(0)]},
         {1, [started(3), stopped(2), crashed(1), started(0)]}
    ]].


last_started_test_() ->
    [?_assertEqual({0, R, 0}, last_started(testjob(H))) || {R, H} <- [
         {0, [added()]},
         {0, [crashed(1)]},
         {1, [started(1)]},
         {1, [added(), started(1)]},
         {2, [started(2), started(1)]},
         {2, [crashed(3), started(2), started(1)]}
    ]].


longest_running_test() ->
    J0 = testjob([crashed()]),
    J1 = testjob([started(1)]),
    J2 = testjob([started(2)]),
    Sort = fun(Jobs) -> lists:sort(fun longest_running/2, Jobs) end,
    ?assertEqual([], Sort([])),
    ?assertEqual([J1], Sort([J1])),
    ?assertEqual([J1, J2], Sort([J2, J1])),
    ?assertEqual([J0, J1, J2], Sort([J2, J1, J0])).


scheduler_test_() ->
    {
        setup,
        fun setup_all/0,
        fun teardown_all/1,
        {
            foreach,
            fun setup/0,
            fun teardown/1,
            [
                t_pending_jobs_simple(),
                t_pending_jobs_skip_crashed(),
                t_one_job_starts(),
                t_no_jobs_start_if_max_is_0(),
                t_one_job_starts_if_max_is_1(),
                t_max_churn_does_not_throttle_initial_start(),
                t_excess_oneshot_only_jobs(),
                t_excess_continuous_only_jobs(),
                t_excess_prefer_continuous_first(),
                t_stop_oldest_first(),
                t_start_oldest_first(),
                t_jobs_churn_even_if_not_all_max_jobs_are_running(),
                t_jobs_dont_churn_if_there_are_available_running_slots(),
                t_start_only_pending_jobs_do_not_churn_existing_ones(),
                t_dont_stop_if_nothing_pending(),
                t_max_churn_limits_number_of_rotated_jobs(),
                t_existing_jobs(),
                t_if_pending_less_than_running_start_all_pending(),
                t_running_less_than_pending_swap_all_running(),
                t_oneshot_dont_get_rotated(),
                t_rotate_continuous_only_if_mixed(),
                t_oneshot_dont_get_starting_priority(),
                t_oneshot_will_hog_the_scheduler(),
                t_if_excess_is_trimmed_rotation_still_happens(),
                t_if_transient_job_crashes_it_gets_removed(),
                t_if_permanent_job_crashes_it_stays_in_ets(),
                t_job_summary_running(),
                t_job_summary_pending(),
                t_job_summary_crashing_once(),
                t_job_summary_crashing_many_times(),
                t_job_summary_proxy_fields()
            ]
        }
    }.


t_pending_jobs_simple() ->
   ?_test(begin
        Job1 = oneshot(1),
        Job2 = oneshot(2),
        setup_jobs([Job2, Job1]),
        ?assertEqual([], pending_jobs(0)),
        ?assertEqual([Job1], pending_jobs(1)),
        ?assertEqual([Job1, Job2], pending_jobs(2)),
        ?assertEqual([Job1, Job2], pending_jobs(3))
    end).


t_pending_jobs_skip_crashed() ->
   ?_test(begin
        Job = oneshot(1),
        Ts = os:timestamp(),
        History = [crashed(Ts), started(Ts) | Job#job.history],
        Job1 = Job#job{history = History},
        Job2 = oneshot(2),
        Job3 = oneshot(3),
        setup_jobs([Job2, Job1, Job3]),
        ?assertEqual([Job2], pending_jobs(1)),
        ?assertEqual([Job2, Job3], pending_jobs(2)),
        ?assertEqual([Job2, Job3], pending_jobs(3))
    end).


t_one_job_starts() ->
    ?_test(begin
        setup_jobs([oneshot(1)]),
        ?assertEqual({0, 1}, run_stop_count()),
        reschedule(mock_state(?DEFAULT_MAX_JOBS)),
        ?assertEqual({1, 0}, run_stop_count())
    end).


t_no_jobs_start_if_max_is_0() ->
    ?_test(begin
        setup_jobs([oneshot(1)]),
        reschedule(mock_state(0)),
        ?assertEqual({0, 1}, run_stop_count())
    end).


t_one_job_starts_if_max_is_1() ->
    ?_test(begin
        setup_jobs([oneshot(1), oneshot(2)]),
        reschedule(mock_state(1)),
        ?assertEqual({1, 1}, run_stop_count())
    end).


t_max_churn_does_not_throttle_initial_start() ->
    ?_test(begin
        setup_jobs([oneshot(1), oneshot(2)]),
        reschedule(mock_state(?DEFAULT_MAX_JOBS, 0)),
        ?assertEqual({2, 0}, run_stop_count())
    end).


t_excess_oneshot_only_jobs() ->
    ?_test(begin
        setup_jobs([oneshot_running(1), oneshot_running(2)]),
        ?assertEqual({2, 0}, run_stop_count()),
        reschedule(mock_state(1)),
        ?assertEqual({1, 1}, run_stop_count()),
        reschedule(mock_state(0)),
        ?assertEqual({0, 2}, run_stop_count())
    end).


t_excess_continuous_only_jobs() ->
    ?_test(begin
        setup_jobs([continuous_running(1), continuous_running(2)]),
        ?assertEqual({2, 0}, run_stop_count()),
        reschedule(mock_state(1)),
        ?assertEqual({1, 1}, run_stop_count()),
        reschedule(mock_state(0)),
        ?assertEqual({0, 2}, run_stop_count())
    end).


t_excess_prefer_continuous_first() ->
    ?_test(begin
        Jobs = [
            continuous_running(1),
            oneshot_running(2),
            continuous_running(3)
        ],
        setup_jobs(Jobs),
        ?assertEqual({3, 0}, run_stop_count()),
        ?assertEqual({1, 0}, oneshot_run_stop_count()),
        reschedule(mock_state(2)),
        ?assertEqual({2, 1}, run_stop_count()),
        ?assertEqual({1, 0}, oneshot_run_stop_count()),
        reschedule(mock_state(1)),
        ?assertEqual({1, 0}, oneshot_run_stop_count()),
        reschedule(mock_state(0)),
        ?assertEqual({0, 1}, oneshot_run_stop_count())
    end).


t_stop_oldest_first() ->
    ?_test(begin
        Jobs = [
            continuous_running(7),
            continuous_running(4),
            continuous_running(5)
        ],
        setup_jobs(Jobs),
        reschedule(mock_state(2, 1)),
        ?assertEqual({2, 1}, run_stop_count()),
        ?assertEqual([4], jobs_stopped()),
        reschedule(mock_state(1, 1)),
        ?assertEqual([7], jobs_running())
    end).


t_start_oldest_first() ->
    ?_test(begin
        setup_jobs([continuous(7), continuous(2), continuous(5)]),
        reschedule(mock_state(1)),
        ?assertEqual({1, 2}, run_stop_count()),
        ?assertEqual([2], jobs_running()),
        reschedule(mock_state(2)),
        ?assertEqual({2, 1}, run_stop_count()),
        % After rescheduling with max_jobs = 2, 2 was stopped and 5, 7 should
        % be running.
        ?assertEqual([2], jobs_stopped())
    end).


t_jobs_churn_even_if_not_all_max_jobs_are_running() ->
    ?_test(begin
        setup_jobs([
            continuous_running(7),
            continuous(2),
            continuous(5)
        ]),
        reschedule(mock_state(2, 2)),
        ?assertEqual({2, 1}, run_stop_count()),
        ?assertEqual([7], jobs_stopped())
    end).


t_jobs_dont_churn_if_there_are_available_running_slots() ->
     ?_test(begin
        setup_jobs([
            continuous_running(1),
            continuous_running(2)
        ]),
        reschedule(mock_state(2, 2)),
        ?assertEqual({2, 0}, run_stop_count()),
        ?assertEqual([], jobs_stopped()),
        ?assertEqual(0, meck:num_calls(couch_replicator_scheduler_sup, start_child, 1))
    end).


t_start_only_pending_jobs_do_not_churn_existing_ones() ->
     ?_test(begin
        setup_jobs([
            continuous(1),
            continuous_running(2)
        ]),
        reschedule(mock_state(2, 2)),
        ?assertEqual(1, meck:num_calls(couch_replicator_scheduler_sup, start_child, 1)),
        ?assertEqual([], jobs_stopped()),
        ?assertEqual({2, 0}, run_stop_count())
    end).


t_dont_stop_if_nothing_pending() ->
    ?_test(begin
        setup_jobs([continuous_running(1), continuous_running(2)]),
        reschedule(mock_state(2)),
        ?assertEqual({2, 0}, run_stop_count())
    end).


t_max_churn_limits_number_of_rotated_jobs() ->
    ?_test(begin
        Jobs = [
            continuous(1),
            continuous_running(2),
            continuous(3),
            continuous_running(4)
        ],
        setup_jobs(Jobs),
        reschedule(mock_state(2, 1)),
        ?assertEqual([2, 3], jobs_stopped())
    end).


t_if_pending_less_than_running_start_all_pending() ->
    ?_test(begin
        Jobs = [
            continuous(1),
            continuous_running(2),
            continuous(3),
            continuous_running(4),
            continuous_running(5)
        ],
        setup_jobs(Jobs),
        reschedule(mock_state(3)),
        ?assertEqual([1, 2, 5], jobs_running())
    end).


t_running_less_than_pending_swap_all_running() ->
    ?_test(begin
        Jobs = [
            continuous(1),
            continuous(2),
            continuous(3),
            continuous_running(4),
            continuous_running(5)
        ],
        setup_jobs(Jobs),
        reschedule(mock_state(2)),
        ?assertEqual([3, 4, 5], jobs_stopped())
    end).


t_oneshot_dont_get_rotated() ->
    ?_test(begin
        setup_jobs([oneshot_running(1), continuous(2)]),
        reschedule(mock_state(1)),
        ?assertEqual([1], jobs_running())
    end).


t_rotate_continuous_only_if_mixed() ->
    ?_test(begin
        setup_jobs([continuous(1), oneshot_running(2), continuous_running(3)]),
        reschedule(mock_state(2)),
        ?assertEqual([1, 2], jobs_running())
    end).


t_oneshot_dont_get_starting_priority() ->
    ?_test(begin
        setup_jobs([continuous(1), oneshot(2), continuous_running(3)]),
        reschedule(mock_state(1)),
        ?assertEqual([1], jobs_running())
    end).


% This tested in other test cases, it is here to mainly make explicit a property
% of one-shot replications -- they can starve other jobs if they "take control"
% of all the available scheduler slots.
t_oneshot_will_hog_the_scheduler() ->
    ?_test(begin
        Jobs = [
            oneshot_running(1),
            oneshot_running(2),
            oneshot(3),
            continuous(4)
        ],
        setup_jobs(Jobs),
        reschedule(mock_state(2)),
        ?assertEqual([1, 2], jobs_running())
    end).


t_if_excess_is_trimmed_rotation_still_happens() ->
    ?_test(begin
        Jobs = [
            continuous(1),
            continuous_running(2),
            continuous_running(3)
        ],
        setup_jobs(Jobs),
        reschedule(mock_state(1)),
        ?assertEqual([1], jobs_running())
    end).


t_if_transient_job_crashes_it_gets_removed() ->
    ?_test(begin
        Pid = mock_pid(),
        Job =  #job{
            id = job1,
            pid = Pid,
            history = [added()],
            rep = #rep{db_name = null, options = [{continuous, true}]}
        },
        setup_jobs([Job]),
        ?assertEqual(1, ets:info(?MODULE, size)),
        State = #state{max_history = 3, stats_pid = self()},
        {noreply, State} = handle_info({'DOWN', r1, process, Pid, failed},
            State),
        ?assertEqual(0, ets:info(?MODULE, size))
   end).


t_if_permanent_job_crashes_it_stays_in_ets() ->
    ?_test(begin
        Pid = mock_pid(),
        Job =  #job{
            id = job1,
            pid = Pid,
            history = [added()],
            rep = #rep{db_name = <<"db1">>, options = [{continuous, true}]}
        },
        setup_jobs([Job]),
        ?assertEqual(1, ets:info(?MODULE, size)),
        State = #state{max_jobs =1, max_history = 3, stats_pid = self()},
        {noreply, State} = handle_info({'DOWN', r1, process, Pid, failed},
            State),
        ?assertEqual(1, ets:info(?MODULE, size)),
        [Job1] = ets:lookup(?MODULE, job1),
        [Latest | _] = Job1#job.history,
        ?assertMatch({{crashed, failed}, _}, Latest)
   end).


t_existing_jobs() ->
    ?_test(begin
        Rep = #rep{
            id = job1,
            db_name = <<"db">>,
            source = <<"s">>,
            target = <<"t">>,
            options = [{continuous, true}]
        },
        setup_jobs([#job{id = Rep#rep.id, rep = Rep}]),
        NewRep = #rep{
            id = Rep#rep.id,
            db_name = <<"db">>,
            source = <<"s">>,
            target = <<"t">>,
            options = [{continuous, true}]
        },
        ?assert(existing_replication(NewRep)),
        ?assertNot(existing_replication(NewRep#rep{source = <<"s1">>})),
        ?assertNot(existing_replication(NewRep#rep{target = <<"t1">>})),
        ?assertNot(existing_replication(NewRep#rep{options = []}))
    end).


t_job_summary_running() ->
    ?_test(begin
        Job =  #job{
            id = job1,
            pid = mock_pid(),
            history = [added()],
            rep = #rep{
                db_name = <<"db1">>,
                source = <<"s">>,
                target = <<"t">>
            }
        },
        setup_jobs([Job]),
        Summary = job_summary(job1, ?DEFAULT_HEALTH_THRESHOLD_SEC),
        ?assertEqual(running, proplists:get_value(state, Summary)),
        ?assertEqual(null, proplists:get_value(info, Summary)),
        ?assertEqual(0, proplists:get_value(error_count, Summary)),

        Stats = [{source_seq, <<"1-abc">>}],
        Summary1 = job_summary(job1, ?DEFAULT_HEALTH_THRESHOLD_SEC),
        ?assertEqual({Stats}, proplists:get_value(info, Summary1))
    end).


t_job_summary_pending() ->
    ?_test(begin
        Job =  #job{
            id = job1,
            pid = undefined,
            history = [stopped(20), started(10), added()],
            rep = #rep{source = <<"s">>, target = <<"t">>}
        },
        setup_jobs([Job]),
        Summary = job_summary(job1, ?DEFAULT_HEALTH_THRESHOLD_SEC),
        ?assertEqual(pending, proplists:get_value(state, Summary)),
        ?assertEqual(null, proplists:get_value(info, Summary)),
        ?assertEqual(0, proplists:get_value(error_count, Summary)),

        Stats = [{doc_write_failures, 1}],
        Summary1 = job_summary(job1, ?DEFAULT_HEALTH_THRESHOLD_SEC),
        ?assertEqual({Stats}, proplists:get_value(info, Summary1))
    end).


t_job_summary_crashing_once() ->
    ?_test(begin
        Job =  #job{
            id = job1,
            history = [crashed(?DEFAULT_HEALTH_THRESHOLD_SEC + 1), started(0)],
            rep = #rep{source = <<"s">>, target = <<"t">>}
        },
        setup_jobs([Job]),
        Summary = job_summary(job1, ?DEFAULT_HEALTH_THRESHOLD_SEC),
        ?assertEqual(crashing, proplists:get_value(state, Summary)),
        Info = proplists:get_value(info, Summary),
        ?assertEqual({[{<<"error">>, <<"some_reason">>}]}, Info),
        ?assertEqual(0, proplists:get_value(error_count, Summary))
    end).


t_job_summary_crashing_many_times() ->
    ?_test(begin
        Job =  #job{
            id = job1,
            history = [crashed(4), started(3), crashed(2), started(1)],
            rep = #rep{source = <<"s">>, target = <<"t">>}
        },
        setup_jobs([Job]),
        Summary = job_summary(job1, ?DEFAULT_HEALTH_THRESHOLD_SEC),
        ?assertEqual(crashing, proplists:get_value(state, Summary)),
        Info = proplists:get_value(info, Summary),
        ?assertEqual({[{<<"error">>, <<"some_reason">>}]}, Info),
        ?assertEqual(2, proplists:get_value(error_count, Summary))
    end).


t_job_summary_proxy_fields() ->
    ?_test(begin
        Job =  #job{
            id = job1,
            history = [started(10), added()],
            rep = #rep{
                source = #httpdb{
                    url = "https://s",
                    proxy_url = "http://u:p@sproxy:12"
                },
                target = #httpdb{
                    url = "http://t",
                    proxy_url = "socks5://u:p@tproxy:34"
                }
            }
        },
        setup_jobs([Job]),
        Summary = job_summary(job1, ?DEFAULT_HEALTH_THRESHOLD_SEC),
        ?assertEqual(<<"http://u:*****@sproxy:12">>,
            proplists:get_value(source_proxy, Summary)),
        ?assertEqual(<<"socks5://u:*****@tproxy:34">>,
            proplists:get_value(target_proxy, Summary))
    end).


% Test helper functions

setup_all() ->
    catch ets:delete(?MODULE),
    meck:expect(couch_log, notice, 2, ok),
    meck:expect(couch_log, warning, 2, ok),
    meck:expect(couch_log, error, 2, ok),
    meck:expect(couch_replicator_scheduler_sup, terminate_child, 1, ok),
    meck:expect(couch_stats, increment_counter, 1, ok),
    meck:expect(couch_stats, update_gauge, 2, ok),
    Pid = mock_pid(),
    meck:expect(couch_replicator_scheduler_sup, start_child, 1, {ok, Pid}).


teardown_all(_) ->
    catch ets:delete(?MODULE),
    meck:unload().


setup() ->
    meck:reset([
        couch_log,
        couch_replicator_scheduler_sup,
        couch_stats
    ]).


teardown(_) ->
    ok.


setup_jobs(Jobs) when is_list(Jobs) ->
    ?MODULE = ets:new(?MODULE, [named_table, {keypos, #job.id}]),
    ets:insert(?MODULE, Jobs).


all_jobs() ->
    lists:usort(ets:tab2list(?MODULE)).


jobs_stopped() ->
    [Job#job.id || Job <- all_jobs(), Job#job.pid =:= undefined].


jobs_running() ->
    [Job#job.id || Job <- all_jobs(), Job#job.pid =/= undefined].


run_stop_count() ->
    {length(jobs_running()), length(jobs_stopped())}.


oneshot_run_stop_count() ->
    Running = [Job#job.id || Job <- all_jobs(), Job#job.pid =/= undefined,
        not is_continuous(Job)],
    Stopped = [Job#job.id || Job <- all_jobs(), Job#job.pid =:= undefined,
        not is_continuous(Job)],
    {length(Running), length(Stopped)}.


mock_state(MaxJobs) ->
    #state{
        max_jobs = MaxJobs,
        max_churn = ?DEFAULT_MAX_CHURN,
        max_history = ?DEFAULT_MAX_HISTORY,
        stats_pid = self()
    }.

mock_state(MaxJobs, MaxChurn) ->
    #state{
        max_jobs = MaxJobs,
        max_churn = MaxChurn,
        max_history = ?DEFAULT_MAX_HISTORY,
        stats_pid = self()
    }.


continuous(Id) when is_integer(Id) ->
    Started = Id,
    Hist = [stopped(Started+1), started(Started), added()],
    #job{
        id = Id,
        history = Hist,
        rep = #rep{options = [{continuous, true}]}
    }.


continuous_running(Id) when is_integer(Id) ->
    Started = Id,
    Pid = mock_pid(),
    #job{
        id = Id,
        history = [started(Started), added()],
        rep = #rep{options = [{continuous, true}]},
        pid = Pid,
        monitor = monitor(process, Pid)
    }.


oneshot(Id) when is_integer(Id) ->
    Started = Id,
    Hist = [stopped(Started + 1), started(Started), added()],
    #job{id = Id, history = Hist, rep = #rep{options = []}}.


oneshot_running(Id) when is_integer(Id) ->
    Started = Id,
    Pid = mock_pid(),
    #job{
        id = Id,
        history = [started(Started), added()],
        rep = #rep{options = []},
        pid = Pid,
        monitor = monitor(process, Pid)
    }.


testjob(Hist) when is_list(Hist) ->
    #job{history = Hist}.


mock_pid() ->
   list_to_pid("<0.999.999>").

crashed() ->
    crashed(0).


crashed(WhenSec) when is_integer(WhenSec)->
    {{crashed, some_reason}, {0, WhenSec, 0}};
crashed({MSec, Sec, USec}) ->
    {{crashed, some_reason}, {MSec, Sec, USec}}.


started() ->
    started(0).


started(WhenSec) when is_integer(WhenSec)->
    {started, {0, WhenSec, 0}};

started({MSec, Sec, USec}) ->
    {started, {MSec, Sec, USec}}.


stopped() ->
    stopped(0).


stopped(WhenSec) ->
    {stopped, {0, WhenSec, 0}}.


added() ->
    {added, {0, 0, 0}}.

-endif.
