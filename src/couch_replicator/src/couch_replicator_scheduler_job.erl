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

-module(couch_replicator_scheduler_job).

-behaviour(gen_server).

-export([
   start_link/3
]).

-export([
   accept/0
]).

-export([
   init/1,
   terminate/2,
   handle_call/3,
   handle_info/2,
   handle_cast/2,
   code_change/3,
   format_status/2
]).

-include_lib("couch/include/couch_db.hrl").
-include_lib("couch_replicator/include/couch_replicator_api_wrap.hrl").
-include("couch_replicator_scheduler.hrl").
-include("couch_replicator.hrl").

-import(couch_util, [
    get_value/2,
    get_value/3,
    to_binary/1
]).


-define(LOWEST_SEQ, 0).
-define(DEFAULT_CHECKPOINT_INTERVAL, 30000).
-define(INITIAL_BACKOFF_EXPONENT, 64).
-define(STARTUP_JITTER_DEFAULT, 5000).
-define(ACCEPT_JITTER_DEFAULT, 5000).
-define(INITIAL_BACKOFF_EXPONENT, 64).
-define(ERROR_MAX_BACKOFF_EXPONENT, 10).
-define(DEFAULT_MIN_BACKOFF_PENALTY_SEC, 32).
-define(DEFAULT_MAX_BACKOFF_PENALTY_SEC, 24 * 3600).
-define(DEFAULT_HEALTH_THRESHOLD_SEC, 2 * 60).
-define(DEFAULT_MAX_HISTORY, 20).


-record(rep_state, {
    job,
    job_data,
    id,
    base_id,
    doc_id,
    db_name,
    db_uuid,
    source_name,
    target_name,
    source,
    target,
    history,
    checkpoint_history,
    start_seq,
    committed_seq,
    current_through_seq,
    seqs_in_progress = [],
    highest_seq_done = {0, ?LOWEST_SEQ},
    source_log,
    target_log,
    rep_starttime,
    src_starttime,
    tgt_starttime,
    timer, % checkpoint timer
    changes_queue,
    changes_manager,
    changes_reader,
    workers,
    stats = couch_replicator_stats:new(),
    session_id,
    source_seq = nil,
    use_checkpoints = true,
    checkpoint_interval = ?DEFAULT_CHECKPOINT_INTERVAL,
    user = null,
    options = #{}
}).


start_link() ->
    gen_server:start_link(?MODULE, [], []).


init(_) ->
    process_flag(trap_exit, true),
    {ok, delayed_init, 0}.


accept() ->
    NowSec = erlang:system_time(second),
    MaxSchedTime = NowSec + accept_jitter() div 1000},
    case couch_replicator_jobs:accept(MaxSchedTime) of
        {ok, Job, #{?REP := Rep} = JobData} ->
            Normal = case Rep of
                #{?OPTIONS := #{} = Options} ->
                    not map:get(<<"continuous">>, Options, false);
                _ ->
                    true
            end,
            couch_replicator_job_server:accepted(self(), Normal),
            {ok, Job, JobData};
        {error, not_found} ->
            timer:sleep(accept_jitter()),
            ?MODULE:accept()
    end.


delayed_init() ->
    couch_log:debug("~p : starting acceptor ", [?MODULE]),
    {ok, Job1, JobData1} = accept(),
    couch_log:debug("~p : accepted job ~p, initializing", [?MODULE, Job]),

    % This may make a network request, then may fail and reschedule the job
    {RepId, BaseId} = get_rep_id(JobData1),

    {?REP : = Rep, ?STATE := LastState, ?STATE_INFO := LastInfo} = JobData1,
    JobId = couch_replicator_ids:job_id(Rep),

    {Job3, JobData3} = couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(), fun(JTx) ->
        remove_old_state_fields(JobData1),
        finish_if_failed(JTx, Job1, JobData1),
        {Job2, JobData2} = set_running_state(JTx, Job1, JobData1, RepId, BaseId),
        assert_ownership(JTx, Job2, JobData2)
    end),

    State = #rep_state{} = do_init(Job3, JobData3),
    {ok, State}.


do_init(Job, #{} = JobData) ->
    #rep_state{
        source = Source,
        target = Target,
        source_name = SourceName,
        target_name = TargetName,
        start_seq = {_Ts, StartSeq},
        highest_seq_done = {_, HighestSeq},
        checkpoint_interval = CheckpointInterval,
        user = User,
        options = Options,
        doc_id = DocId,
        db_name = DbName
    } = State = init_state(Job, JobData),

    NumWorkers = get_value(worker_processes, Options),
    BatchSize = get_value(worker_batch_size, Options),
    {ok, ChangesQueue} = couch_work_queue:new([
        {max_items, BatchSize * NumWorkers * 2},
        {max_size, 100 * 1024 * NumWorkers}
    ]),
    % This starts the _changes reader process. It adds the changes from
    % the source db to the ChangesQueue.
    {ok, ChangesReader} = couch_replicator_changes_reader:start_link(
        StartSeq, Source, ChangesQueue, Options
    ),
    % Changes manager - responsible for dequeing batches from the changes queue
    % and deliver them to the worker processes.
    ChangesManager = spawn_changes_manager(self(), ChangesQueue, BatchSize),
    % This starts the worker processes. They ask the changes queue manager for a
    % a batch of _changes rows to process -> check which revs are missing in the
    % target, and for the missing ones, it copies them from the source to the target.
    MaxConns = get_value(http_connections, Options),
    Workers = lists:map(
        fun(_) ->
            couch_stats:increment_counter([couch_replicator, workers_started]),
            {ok, Pid} = couch_replicator_worker:start_link(
                self(), Source, Target, ChangesManager, MaxConns),
            Pid
        end,
        lists:seq(1, NumWorkers)),

    % TODO: replace with new task job state
    couch_task_status:add_task([
        {type, replication},
        {user, User},
        {replication_id, State#rep_state.id},
        {database, DbName},
        {doc_id, DocId},
        {source, ?l2b(SourceName)},
        {target, ?l2b(TargetName)},
        {continuous, get_value(continuous, Options, false)},
        {source_seq, HighestSeq},
        {checkpoint_interval, CheckpointInterval}
    ] ++ rep_stats(State)),
    couch_task_status:set_update_frequency(1000),

    log_replication_start(State),
    couch_log:debug("Worker pids are: ~p", [Workers]),

    doc_update_triggered(Rep),

    {ok, State#rep_state{
            changes_queue = ChangesQueue,
            changes_manager = ChangesManager,
            changes_reader = ChangesReader,
            workers = Workers
        }
    }.


finish_if_failed(Job, #{} = JobData) ->
    case JobData of
        #{?REP := null, ?STATE := ?ST_FAILED, ?STATE_INFO := Error} ->
            ok = fail_job(Job, JobData),
            throw(finished);
        #{?REP := #{}} ->
            ok
    end.


remove_old_state_fields(#{?DOC_STATE := DocState} = JobData) when
        DocState =:= ?TRIGGERED orelse DocState =:= ?ERROR ->
    case update_docs() of
        true ->
            ok;
        false ->
            #{?REP := Rep, ?DB_NAME := DbName, ?DOC_ID := DocId} = JobData,
            case is_binary(DbName) andalso is_binary(DocId) of
                true ->
                    couch_replicator_docs:remove_state_fields(DbName, DocId);
                false ->
                    ok
            end
    end;

remove_old_state_fields(#{}) ->
    ok.


set_running_state(_, Job, #{?REP_ID := RepId} = JobData, RepId, _) ->
    {Job, JobData};

set_running_state(#{jtx := true} = JTx, Job, #{} = JobData, RepId, BaseId) ->
    #{?REP: = Rep, ?REP_ID := OldRepId, ?JOB_HISTORY := Hist} = JobData,
    JobId = couch_replicator_ids:job_id(Rep),
    ok = couch_replicator:clear_old_rep_id(JTx, JobId, OldRepId),
    NowSec = erlang:system_time(second),
    JobData1 = JobData#{
        ?REP_ID := RepId,
        ?BASE_ID := BaseId,
        ?STATE := ?ST_RUNNING,
        ?STATE_INFO := null,
        ?LAST_START_TIME := NowSec
    },
    JobData2 = hist_append(?HIST_STARTED, JobData1, NowSec, undefined),
    couch_stats:increment_counter([couch_replicator, jobs, starts]),
    update_job_data(JTx, Job, JobData2).


assert_ownership(#{jtx := true} = JTx, Job, JobData) ->
    % Check that we this job should still be running this repliction or maybe
    % there is other which should do it
    #{?REP_ID := RepId, ?REP := Rep} = JobData,
    JobId = couch_replicator_ids:job_id(Rep),
    case couch_replicator_jobs:try_update_rep_id(JTx, JobId, RepId) of
        ok ->
            ok;
        {error, {replication_job_conflict, OtherJobId}} ->
            case couch_replicator_jobs:get_job_data(JTx, OtherJobId) of
                {ok, #{?STATE := ?ST_RUNNING}} ->
                    Error = <<"Duplicate job running: ", OtherJobId/binary>>,
                    reschedule_job_on_error(JTx, Job, JobData, Error),
                    throw(finished);
                {ok, #{?STATE := ?ST_PENDING}} ->
                    Error = <<"Duplicate job pending: ", OtherJobId/binary>>,
                    reschedule_job_on_error(JTx, Job, JobData, Error),
                    throw(finished);
                {ok, #{}} ->
                    LogMsg = "~p : Job ~p usurping job ~p for replication ~p",
                    couch_log:warning(LogMsg, [?MODULE, JobId, OtherJobId]),
                    ok = couch_replicator_jobs:update_rep_id(JTx, JobId, RepId)
                {error, not_found} ->
                     LogMsg = "~p : Orphan replication job reference ~p -> ~p",
                     couch_log:error(LogMsg, [?MODULE, RepId, OtherJobId]),
                     ok = couch_replicator_jobs:update_rep_id(JTx, JobId, RepId)
             end
    end.


update_job_data(#{jtx := true} = JTx, Job, JobData) ->
    case couch_replicator_job:update_job_data(JTx, Job, JobData) of
        {ok, Job1} -> {Job1, JobData};
        {error, halt} -> throw(halt)
    end.


reschedule_job_on_error(JTx, Job, JobData0, Error0) ->
    NowSec = erlang:system_time(second),

    JobData = maybe_heal(JobData0, NowSec),

    #{?ERROR_COUNT := ErrorCount, ?JOB_HISTORY := Hist} = JobData,
    ErrorCount1 = ErrorCount + 1,

    Error = case Error0 of
        <<_/binary>> -> Error0;
        undefined -> undefined;
        null -> null;
        Other -> couch_replicator_util:rep_error_to_binary(Error0)
    end,

    JobData1 = JobData#{
        ?STATE := ?ST_CRASHING,
        ?STATE_INFO := Error,
        ?ERROR_COUNT := ErrorCount1,
        ?LAST_ERROR := Error
    },
    JobData2 = hist_append(?HIST_CRASHED, NowSec, JobData1, Error),
    JobData3 = hist_append(?HIST_PENDING, NowSec, JobData2, undefined),

    % TODO: permanently fail and delete transient jobs

    couch_stats:increment_counter([couch_replicator, jobs, crashes]),

    Time = get_backoff_time(ErrorCount1),
    case couch_replicator_job:reschedule_job(JTx, Job, JobData3, Time) of
        ok -> ok;
        {error, halt} -> throw(halt)
    end.


reschedule_job(JTx, Job, JobData) ->
    NowSec = erlang:system_time(second),

    JobData1 = JobData#{
        ?STATE := ?ST_PENDING,
        ?STATE_INFO := null,
        ?LAST_ERROR := null,
    },
    JobData2 = hist_append(?HIST_STOPPED, NowSec, JobData1, undefined),
    JobData3 = hist_append(?HIST_PENDING, NowSec, JobData2, undefined),

    % TODO: and delete transient jobs
    couch_stats:increment_counter([couch_replicator, jobs, stops]),

    Time = erlang:system_time(second),
    case couch_replicator_job:reschedule_job(JTx, Job, JobData3, Time) of
        ok -> ok;
        {error, halt} -> throw(halt)
    end.


fail_job(Job, JobData, Error) ->
    NowSec = erlang:system_time(second),

    #{?ERROR_COUNT := ErrorCount} = JobData,

    JobData1 = JobData#{
        ?STATE := ?ST_FAILED,
        ?STATE_INFO := Error,
        ?ERROR_COUNT := ErrorCount + 1,
    },

    JobData2 = hist_append(?HIST_CRASHED, NowSEc, JobDat1, Error),

    % TODO: maybe delete transient jobs here
    case couch_replicator_jobs:finish_job(undefined, Job, JobData2) of
        ok -> ok;
        {error, halt} -> throw(halt)
    end.


get_rep_id(JTx, Job, #{} = JobData) ->
    #{?REP := Rep} = JobData,
    try
        couch_replicator_ids:replication_id(Rep),
    catch
        throw:{filter_fetch_error, Error} ->
            Error1 = io_lib:format("Filter fetch error ~p", [Error]),
            Error2 = couch_util:to_binary(Error1),
            reschedule_job_on_error(JTx, Job, JobData, Error2),
            throw(finished)
    end.


maybe_heal_job(#{} = JobData, NowSec) ->
    #{?LAST_START_TIME := LastStartTime} = JobData,
    case NowSec - LastStartTime > health_threashold() of
        true -> JobData#{?ERROR_COUNT := 0};
        false -> JobData
    end.


get_backoff_time(ErrorCount) ->
    Max = min(max_backoff_penalty_sec(), 3600 * 24 * 30),
    Min = max(min_backoff_penalty_sec(), 4),

    % Calculate the max exponent so exponentiation doesn't blow up
    MaxExp = math:log2(Max) - math:log2(Min),

    % This is the recommended backoff amount
    Wait = Min * math:pow(2, min(ErrCnt, MaxExp)),

    % Apply a 25% jitter to avoid a thundering herd effect
    WaitJittered = Wait * 0.75 + rand:uniform(trunc(Wait * 0.25) + 1),

    erlang:system_time(second) + trunc(WaitJittered).


maybe_update_doc_error(RepId, DbName, DocId, Error) ->
    case update_docs() of
        true when is_binary(DbName), is_binary(DocId) ->
            couch_replicator_docs:update_error(RepId, DbName, DocId, Error);
        _ ->
            ok
    end.


maybe_update_doc_triggered(RepId, DbName, DocId) ->
    case update_docs() of
        true when is_binary(DbName), is_binary(DocId) ->
            couch_replicator_docs:update_triggered(RepId, DbName, DocId);
        _ ->
            ok
    end.


handle_call({add_stats, Stats}, From, State) ->
    gen_server:reply(From, ok),
    NewStats = couch_replicator_utils:sum_stats(State#rep_state.stats, Stats),
    {noreply, State#rep_state{stats = NewStats}};

handle_call({report_seq_done, Seq, StatsInc}, From,
    #rep_state{seqs_in_progress = SeqsInProgress, highest_seq_done = HighestDone,
        current_through_seq = ThroughSeq, stats = Stats} = State) ->
    gen_server:reply(From, ok),
    {NewThroughSeq0, NewSeqsInProgress} = case SeqsInProgress of
    [] ->
        {Seq, []};
    [Seq | Rest] ->
        {Seq, Rest};
    [_ | _] ->
        {ThroughSeq, ordsets:del_element(Seq, SeqsInProgress)}
    end,
    NewHighestDone = lists:max([HighestDone, Seq]),
    NewThroughSeq = case NewSeqsInProgress of
    [] ->
        lists:max([NewThroughSeq0, NewHighestDone]);
    _ ->
        NewThroughSeq0
    end,
    couch_log:debug("Worker reported seq ~p, through seq was ~p, "
        "new through seq is ~p, highest seq done was ~p, "
        "new highest seq done is ~p~n"
        "Seqs in progress were: ~p~nSeqs in progress are now: ~p",
        [Seq, ThroughSeq, NewThroughSeq, HighestDone,
            NewHighestDone, SeqsInProgress, NewSeqsInProgress]),
    NewState = State#rep_state{
        stats = couch_replicator_utils:sum_stats(Stats, StatsInc),
        current_through_seq = NewThroughSeq,
        seqs_in_progress = NewSeqsInProgress,
        highest_seq_done = NewHighestDone
    },
    {noreply, update_job_state(NewState)};

handle_call(Msg, _From, St) ->
    {stop, {bad_call, Msg}, {bad_call, Msg}, St}.


handle_cast({report_seq, Seq},
    #rep_state{seqs_in_progress = SeqsInProgress} = State) ->
    NewSeqsInProgress = ordsets:add_element(Seq, SeqsInProgress),
    {noreply, State#rep_state{seqs_in_progress = NewSeqsInProgress}};

handle_cast(Msg, St) ->
    {stop, {bad_cast, Msg}, St}.


handle_info(checkpoint, State) ->
    ok = check_user_filter(State),
    case do_checkpoint(State) of
        {ok, State1} ->
            couch_stats:increment_counter([couch_replicator, checkpoints,
                success]),
            {noreply, start_timer(State1)};
        Error ->
            couch_stats:increment_counter([couch_replicator, checkpoints,
                failure]),
            {stop, Error, State}
    end;

handle_info(shutdown, St) ->
    {stop, shutdown, St};

handle_info({'EXIT', Pid, max_backoff}, State) ->
    couch_log:error("Max backoff reached child process ~p", [Pid]),
    {stop, {shutdown, max_backoff}, State};

handle_info({'EXIT', Pid, {shutdown, max_backoff}}, State) ->
    couch_log:error("Max backoff reached child process ~p", [Pid]),
    {stop, {shutdown, max_backoff}, State};

handle_info({'EXIT', Pid, normal}, #rep_state{changes_reader=Pid} = State) ->
    {noreply, State};

handle_info({'EXIT', Pid, Reason0}, #rep_state{changes_reader=Pid} = State) ->
    couch_stats:increment_counter([couch_replicator, changes_reader_deaths]),
    Reason = case Reason0 of
        {changes_req_failed, _, _} = HttpFail ->
            HttpFail;
        {http_request_failed, _, _, {error, {code, Code}}} ->
            {changes_req_failed, Code};
        {http_request_failed, _, _, {error, Err}} ->
            {changes_req_failed, Err};
        Other ->
            {changes_reader_died, Other}
    end,
    couch_log:error("ChangesReader process died with reason: ~p", [Reason]),
    {stop, {shutdown, Reason}, cancel_timer(State)};

handle_info({'EXIT', Pid, normal}, #rep_state{changes_manager = Pid} = State) ->
    {noreply, State};

handle_info({'EXIT', Pid, Reason}, #rep_state{changes_manager = Pid} = State) ->
    couch_stats:increment_counter([couch_replicator, changes_manager_deaths]),
    couch_log:error("ChangesManager process died with reason: ~p", [Reason]),
    {stop, {shutdown, {changes_manager_died, Reason}}, cancel_timer(State)};

handle_info({'EXIT', Pid, normal}, #rep_state{changes_queue=Pid} = State) ->
    {noreply, State};

handle_info({'EXIT', Pid, Reason}, #rep_state{changes_queue=Pid} = State) ->
    couch_stats:increment_counter([couch_replicator, changes_queue_deaths]),
    couch_log:error("ChangesQueue process died with reason: ~p", [Reason]),
    {stop, {shutdown, {changes_queue_died, Reason}}, cancel_timer(State)};

handle_info({'EXIT', Pid, normal}, #rep_state{workers = Workers} = State) ->
    case Workers -- [Pid] of
    Workers ->
        couch_log:error("unknown pid bit the dust ~p ~n",[Pid]),
        {noreply, State#rep_state{workers = Workers}};
        %% not clear why a stop was here before
        %%{stop, {unknown_process_died, Pid, normal}, State};
    [] ->
        catch unlink(State#rep_state.changes_manager),
        catch exit(State#rep_state.changes_manager, kill),
        do_last_checkpoint(State);
    Workers2 ->
        {noreply, State#rep_state{workers = Workers2}}
    end;

handle_info({'EXIT', Pid, Reason}, #rep_state{workers = Workers} = State) ->
    State2 = cancel_timer(State),
    case lists:member(Pid, Workers) of
    false ->
        {stop, {unknown_process_died, Pid, Reason}, State2};
    true ->
        couch_stats:increment_counter([couch_replicator, worker_deaths]),
        StopReason = case Reason of
            {shutdown, _} = Err ->
                Err;
            Other ->
                couch_log:error("Worker ~p died with reason: ~p", [Pid, Reason]),
                {worker_died, Pid, Other}
         end,
        {stop, StopReason, State2}
    end;

handle_info(timeout, delayed_init) ->
    try delayed_init() of
        {ok, State} ->
            {noreply, State}
    catch
        exit:{http_request_failed, _, _, max_backoff} ->
            {stop, {shutdown, max_backoff}, {error, max_backoff}};
        throw:finished ->
            {stop, {shutdown, finished}, finished};
        throw:halt ->
            {stop, {shutdown, halt}, halt};
        Class:Error ->
            ShutdownReason = {error, replication_start_error(Error)},
            StackTop2 = lists:sublist(erlang:get_stacktrace(), 2),
            % Shutdown state is a hack as it is not really the state of the
            % gen_server (it failed to initialize, so it doesn't have one).
            % Shutdown state is used to pass extra info about why start failed.
            ShutdownState = {error, Class, StackTop2, _InitArgs = []},
            {stop, {shutdown, ShutdownReason}, ShutdownState}
    end;


handle_info(Msg, St) ->
    {stop, {bad_info, Msg}, St}.


terminate(normal, #rep_state{} = State) ->
    % Note: when terminating `normal`, the job was already marked as finished.
    % if that fails then we'd end up in the error terminate clause
    terminate_cleanup(State).

terminate(shutdown, #rep_state{id = RepId} = State) ->
    % Replication stopped via _scheduler_sup:terminate_child/1, which can be
    % occur during regular scheduler operation or when job is removed from
    % the scheduler.
    State1 = case do_checkpoint(State) of
        {ok, NewState} ->
            NewState;
        Error ->
            LogMsg = "~p : Failed last checkpoint. Job: ~p Error: ~p",
            couch_log:error(LogMsg, [?MODULE, RepId, Error]),
            State
    end,    finish_couch_job(State1, ?ST_PENDING, null),
    terminate_cleanup(State1);

terminate({shutdown, finished}, finished) ->
    couch_log:notice("~p : Job finished in init", [?MODULE]),
    ok;

terminate({shutdown, halt}, halt) ->
    couch_log:error("~p : Replication job halted", [?MODULE]),
    ok;

terminate({shutdown, max_backoff}, {error, {#{} = Job, #{} = JobData}}) ->
    % Here we handle the case when replication fails during initialization.
    % That is before the #rep_state{} is even built.
    #{?REP_ID := RepId} = JobData,
    couch_stats:increment_counter([couch_replicator, failed_starts]),
    couch_log:warning("Replication `~s` reached max backoff ", [RepId]),
    finish_couch_job(Job, JobData, <<"error">>, max_backoff);

terminate({shutdown, {error, Error}}, {error, Class, Stack, {Job, JobData}}) ->
    % Here we handle the case when replication fails during initialization.
    #{
        ?REP_ID := Id,
        ?DB_NAME := DbName,
        ?DOC_ID := DocId} = JobData,
        ?REP := #{?SOURCE := Source0, ?TARGET := Target0}
    } = JobData,
    Source = couch_replicator_api_wrap:db_uri(Source0),
    Target = couch_replicator_api_wrap:db_uri(Target0),
    Msg = "~p:~p: Replication ~s failed to start ~p -> ~p doc ~p:~p stack:~p",
    couch_log:error(Msg, [Class, Error, RepId, Source, Target, DbName,
        DocId, Stack]),
    couch_stats:increment_counter([couch_replicator, failed_starts]),
    finish_couch_job(Job, JobData, <<"error">>, Error);

terminate({shutdown, max_backoff}, #rep_state{} = State) ->
    #rep_state{
        id = RepId,
        source_name = Source,
        target_name = Target,
    } = State,
    couch_log:error("Replication `~s` (`~s` -> `~s`) reached max backoff",
        [RepId, Source, Target]),
    terminate_cleanup(State),
    finish_couch_job(State, <<"error">>, max_backoff);

terminate(Reason, State) ->
    #rep_state{
        id = RepId,
        source_name = Source,
        target_name = Target,
    } = State,
    couch_log:error("Replication `~s` (`~s` -> `~s`) failed: ~s",
        [RepId, Source, Target, to_binary(Reason)]),
    terminate_cleanup(State),
    finish_couch_job(State, <<"error">>, Reason).


terminate_cleanup(State) ->
    update_job_state(State),
    couch_replicator_api_wrap:db_close(State#rep_state.source),
    couch_replicator_api_wrap:db_close(State#rep_state.target).


code_change(_OldVsn, #rep_state{}=State, _Extra) ->
    {ok, State}.


format_status(_Opt, [_PDict, State]) ->
    #rep_state{
       id = Id,
       source = Source,
       target = Target,
       start_seq = StartSeq,
       source_seq = SourceSeq,
       committed_seq = CommitedSeq,
       current_through_seq = ThroughSeq,
       highest_seq_done = HighestSeqDone,
       session_id = SessionId,
       doc_id = DocId,
       db_name = DbName,
       options = Options
    } = state_strip_creds(State),
    [
        {rep_id, RepId},
        {source, couch_replicator_api_wrap:db_uri(Source)},
        {target, couch_replicator_api_wrap:db_uri(Target)},
        {db_name, DbName},
        {doc_id, DocId},
        {options, Options},
        {session_id, SessionId},
        {start_seq, StartSeq},
        {source_seq, SourceSeq},
        {committed_seq, CommitedSeq},
        {current_through_seq, ThroughSeq},
        {highest_seq_done, HighestSeqDone}
    ].

headers_strip_creds([], Acc) ->
    lists:reverse(Acc);
headers_strip_creds([{Key, Value0} | Rest], Acc) ->
    Value = case string:to_lower(Key) of
    "authorization" ->
        "****";
    _ ->
        Value0
    end,
    headers_strip_creds(Rest, [{Key, Value} | Acc]).


httpdb_strip_creds(#httpdb{url = Url, headers = Headers} = HttpDb) ->
    HttpDb#httpdb{
        url = couch_util:url_strip_password(Url),
        headers = headers_strip_creds(Headers, [])
    };
httpdb_strip_creds(LocalDb) ->
    LocalDb.


state_strip_creds(#rep_state{source = Source, target = Target} = State) ->
    State#rep_state{
        source = httpdb_strip_creds(Source),
        target = httpdb_strip_creds(Target)
    }.


adjust_maxconn(Src = #{<<"http_connections">> : = 1}, RepId) ->
    Msg = "Adjusting minimum number of HTTP source connections to 2 for ~p",
    couch_log:notice(Msg, [RepId]),
    Src#{<<"http_connections">> := 2};
adjust_maxconn(Src, _RepId) ->
    Src.


-spec doc_update_completed(#rep_state{}) -> ok.
doc_update_completed(#rep_state{db_name = null}) ->
    ok;
doc_update_completed(#rep_state{} = State) ->
    #rep_state{
        id = Id,
        doc_id = DocId,
        db_name = DbName,
        start_time = Start,
        stats = Stats0
    } = State,
    Stats = Stats0 ++ [{start_time, couch_replicator_utils:iso8601(Start)}],
    couch_replicator_docs:update_doc_completed(DbName, DocId, Stats),
    couch_log:notice("Replication `~s` completed (triggered by `~s:~s`)",
        [Id, DbName, DocId]),
    ok.


do_last_checkpoint(#rep_state{seqs_in_progress = [],
    highest_seq_done = {_Ts, ?LOWEST_SEQ}} = State) ->
    History = State#rep_state.checkoint_history,
    Result = case finish_couch_job(State, ?ST_COMPLETED, History) of
        ok -> normal;
        {error, _} = Error -> Error
    end,
    {stop, Result, cancel_timer(State)};
do_last_checkpoint(#rep_state{seqs_in_progress = [],
    highest_seq_done = Seq} = State) ->
    case do_checkpoint(State#rep_state{current_through_seq = Seq}) of
    {ok, NewState} ->
        couch_stats:increment_counter([couch_replicator, checkpoints, success]),
        History = NewState#rep_state.checkpoint_history,
        Result = case finish_couch_job(NewState, ?ST_COMPLETED, History) of
            ok -> normal;
            {error, _} = Error -> Error
        end,
        {stop, Result, cancel_timer(NewState)};
    Error ->
        couch_stats:increment_counter([couch_replicator, checkpoints, failure]),
        {stop, Error, State}
    end.


finish_couch_job(#rep_state{} = State, FinishedState, Result) ->
    #rep_state{job = Job, job_data = Jobdata} = State,
    finish_couch_job(Job, JobData, FinishedState, Result).


finish_couch_job(#{} = Job, #{} = JobData, FinishState, Result0) ->
    #{?REP_ID := RepId} = JobData,
    Result = case Result0 of
        null -> null;
        #{} -> Result0;
        <<_/binary>> -> Result0;
        Atom when is_atom(Atom) -> atom_to_binary(Atom, utf8)
        Other -> couch_replicator_utils:rep_error_to_binary(Result0)
    end,
    JobData= JobData0#{
        ?FINISHED_STATE => FinishState,
        ?FINISHED_RESULT => Result
    },
    case couch_jobs:finish(undefined, Job, JobData) of
        ok ->
            doc_update_completed(State),
            ok;
        {error, Error} ->
            Msg = "Replication ~s job could not finish. Error:~p",
            couch_log:error(Msg, [RepId, Error]),
            {error, Error}
    end.


start_timer(#rep_state{} = State) ->
    CheckpointAfterMSec = State#rep_state.checkpoint_interval,
    JobTimeoutMSec = couch_replicator_jobs:get_timeout() * 1000,
    Wait1 = min(CheckpointAfterMSec, JobTimeoutMSec div 2),
    Wait2 = max(5000, Wait1),
    Wait3 = trunc(Wait2 * 0.75) + rand:uniform(trunc(Wait2 * 0.25)),
    TRef = erlang:send_afer(Wait3, self(), checkpoint),
    State#rep_state{timer = TRef}.


cancel_timer(#rep_state{timer = nil} = State) ->
    State;
cancel_timer(#rep_state{timer = Timer} = State) ->
    erlang:cancel_timer(Timer),
    State#rep_state{timer = nil}.


init_state(#{} = Job, #{} = JobData) ->
    #{
        ?REP := Rep,
        ?REP_ID := Id,
        ?BASE_ID := BaseId,
        ?DB_NAME := DbName,
        ?DB_UUID := DbUUID,
        ?DOC_ID := DocId,
        ?LAST_ERROR := LastError
    } = JobData,
    #{
        ?SOURCE := Src0,
        ?TARGET := Tgt,
        ?START_TIME := StartTime,
        ?OPTIONS := OptionsMap0,
    } = Rep,

    % Optimize replication parameters if last time the jobs crashed because it
    % was rate limited
    OptionsMap = optimize_rate_limited_job(OptionsMap0, LastError),

    Options = maps:fold(fun(K, V, Acc) ->
        [{binary_to_atom(K, utf8), V} | Acc]
    end, [], OptionsMap),

    % Adjust minimum number of http source connections to 2 to avoid deadlock
    Src = adjust_maxconn(Src0, BaseId),
    {ok, Source} = couch_replicator_api_wrap:db_open(Src),
    CreateTgt = get_value(create_target, Options, false),
    TParams = maps:to_list(get_value(create_target_params, Options, #{}),
    {ok, Target} = couch_replicator_api_wrap:db_open(Tgt, CreateTgt, TParams),

    {ok, SourceInfo} = couch_replicator_api_wrap:get_db_info(Source),
    {ok, TargetInfo} = couch_replicator_api_wrap:get_db_info(Target),

    [SourceLog, TargetLog] = find_and_migrate_logs([Source, Target], Rep),

    {StartSeq0, History} = compare_replication_logs(SourceLog, TargetLog),

    #{?REP_STATS := Stats0} = JobData,
    Stats1 = couch_replicator_stats:new(Stats0),
    HistoryStats = case History of
        [{[_ | _] = HProps} | _] -> couch_replicator_stats:new(HProps);
        _ -> couch_replicator_stats:new()
    end,
    Stats2 = couch_replicator_stats:max_stats(Stats1, HistoryStats),

    StartSeq1 = get_value(since_seq, Options, StartSeq0),
    StartSeq = {0, StartSeq1},

    SourceSeq = get_value(<<"update_seq">>, SourceInfo, ?LOWEST_SEQ),

    #doc{body={CheckpointHistory}} = SourceLog,
    State = #rep_state{
        job = Job,
        job_data = JobData,
        id = Id,
        base_id = BaseId,
        source_name = couch_replicator_api_wrap:db_uri(Source),
        target_name = couch_replicator_api_wrap:db_uri(Target),
        source = Source,
        target = Target,
        history = History,
        checkpoint_history = {[{<<"no_changes">>, true}| CheckpointHistory]},
        start_seq = StartSeq,
        current_through_seq = StartSeq,
        committed_seq = StartSeq,
        source_log = SourceLog,
        target_log = TargetLog,
        rep_starttime = StartTime,
        src_starttime = get_value(<<"instance_start_time">>, SourceInfo),
        tgt_starttime = get_value(<<"instance_start_time">>, TargetInfo),
        session_id = couch_uuids:random(),
        source_seq = SourceSeq,
        use_checkpoints = get_value(use_checkpoints, Options),
        checkpoint_interval = get_value(checkpoint_interval, Options),
        stats = Stats2,
        doc_id = DocId,
        db_name = DbName,
        db_uuid = DbUUID
    },
    start_timer(State).


find_and_migrate_logs(DbList, #{?BASE_ID := BaseId} = Rep) ->
    LogId = ?l2b(?LOCAL_DOC_PREFIX ++ BaseId),
    fold_replication_logs(DbList, ?REP_ID_VERSION, LogId, LogId, State, []).


fold_replication_logs([], _Vsn, _LogId, _NewId, _Rep, Acc) ->
    lists:reverse(Acc);

fold_replication_logs([Db | Rest] = Dbs, Vsn, LogId, NewId, #{} = Rep, Acc) ->
    case couch_replicator_api_wrap:open_doc(Db, LogId, [ejson_body]) of
    {error, <<"not_found">>} when Vsn > 1 ->
        OldRepId = couch_replicator_utils:replication_id(Rep, Vsn - 1),
        fold_replication_logs(Dbs, Vsn - 1,
            ?l2b(?LOCAL_DOC_PREFIX ++ OldRepId), NewId, Rep, Acc);
    {error, <<"not_found">>} ->
        fold_replication_logs(
            Rest, ?REP_ID_VERSION, NewId, NewId, Rep, [#doc{id = NewId} | Acc]);
    {ok, Doc} when LogId =:= NewId ->
        fold_replication_logs(
            Rest, ?REP_ID_VERSION, NewId, NewId, Rep, [Doc | Acc]);
    {ok, Doc} ->
        MigratedLog = #doc{id = NewId, body = Doc#doc.body},
        maybe_save_migrated_log(Rep, Db, MigratedLog, Doc#doc.id),
        fold_replication_logs(
            Rest, ?REP_ID_VERSION, NewId, NewId, Rep, [MigratedLog | Acc])
    end.


maybe_save_migrated_log(#{?OPTIONS := Options}, Db, #doc{} = Doc, OldId) ->
    case maps:get(<<"use_checkpoints">>, Options) of
        true ->
            update_checkpoint(Db, Doc),
            Msg = "Migrated replication checkpoint. Db:~p ~p -> ~p",
            couch_log:notice(Msg, [httpdb_strip_creds(Db), OldId, Doc#doc.id]);
        false ->
            ok
    end.


spawn_changes_manager(Parent, ChangesQueue, BatchSize) ->
    spawn_link(fun() ->
        changes_manager_loop_open(Parent, ChangesQueue, BatchSize, 1)
    end).


changes_manager_loop_open(Parent, ChangesQueue, BatchSize, Ts) ->
    receive
    {get_changes, From} ->
        case couch_work_queue:dequeue(ChangesQueue, BatchSize) of
        closed ->
            From ! {closed, self()};
        {ok, ChangesOrLastSeqs} ->
            ReportSeq = case lists:last(ChangesOrLastSeqs) of
                {last_seq, Seq} ->
                    {Ts, Seq};
                #doc_info{high_seq = Seq} ->
                    {Ts, Seq}
            end,
            Changes = lists:filter(
                fun(#doc_info{}) ->
                    true;
                ({last_seq, _Seq}) ->
                    false
            end, ChangesOrLastSeqs),
            ok = gen_server:cast(Parent, {report_seq, ReportSeq}),
            From ! {changes, self(), Changes, ReportSeq}
        end,
        changes_manager_loop_open(Parent, ChangesQueue, BatchSize, Ts + 1)
    end.


do_checkpoint(#rep_state{use_checkpoints=false} = State) ->
    NewState = State#rep_state{checkpoint_history = {[{<<"use_checkpoints">>, false}]} },
    {ok, update_job_state(NewState)};
do_checkpoint(#rep_state{current_through_seq=Seq, committed_seq=Seq} = State) ->
    {ok, update_job_state(State)};
do_checkpoint(State) ->
    #rep_state{
        source_name=SourceName,
        target_name=TargetName,
        source = Source,
        target = Target,
        history = OldHistory,
        start_seq = {_, StartSeq},
        current_through_seq = {_Ts, NewSeq} = NewTsSeq,
        source_log = SourceLog,
        target_log = TargetLog,
        rep_starttime = ReplicationStartTime,
        src_starttime = SrcInstanceStartTime,
        tgt_starttime = TgtInstanceStartTime,
        stats = Stats,
        options = Options,
        session_id = SessionId
    } = State,
    case commit_to_both(Source, Target) of
    {source_error, Reason} ->
         {checkpoint_commit_failure,
             <<"Failure on source commit: ", (to_binary(Reason))/binary>>};
    {target_error, Reason} ->
         {checkpoint_commit_failure,
             <<"Failure on target commit: ", (to_binary(Reason))/binary>>};
    {SrcInstanceStartTime, TgtInstanceStartTime} ->
        couch_log:notice("recording a checkpoint for `~s` -> `~s` at source update_seq ~p",
            [SourceName, TargetName, NewSeq]),
        LocalStartTime = calendar:now_to_local_time(ReplicationStartTime),
        StartTime = ?l2b(httpd_util:rfc1123_date(LocalStartTime)),
        EndTime = ?l2b(httpd_util:rfc1123_date()),
        NewHistoryEntry = {[
            {<<"session_id">>, SessionId},
            {<<"start_time">>, StartTime},
            {<<"end_time">>, EndTime},
            {<<"start_last_seq">>, StartSeq},
            {<<"end_last_seq">>, NewSeq},
            {<<"recorded_seq">>, NewSeq},
            {<<"missing_checked">>, couch_replicator_stats:missing_checked(Stats)},
            {<<"missing_found">>, couch_replicator_stats:missing_found(Stats)},
            {<<"docs_read">>, couch_replicator_stats:docs_read(Stats)},
            {<<"docs_written">>, couch_replicator_stats:docs_written(Stats)},
            {<<"doc_write_failures">>, couch_replicator_stats:doc_write_failures(Stats)}
        ]},
        BaseHistory = [
            {<<"session_id">>, SessionId},
            {<<"source_last_seq">>, NewSeq},
            {<<"replication_id_version">>, ?REP_ID_VERSION}
        ] ++ case get_value(doc_ids, Options) of
        undefined ->
            [];
        _DocIds ->
            % backwards compatibility with the result of a replication by
            % doc IDs in versions 0.11.x and 1.0.x
            % TODO: deprecate (use same history format, simplify code)
            [
                {<<"start_time">>, StartTime},
                {<<"end_time">>, EndTime},
                {<<"docs_read">>, couch_replicator_stats:docs_read(Stats)},
                {<<"docs_written">>, couch_replicator_stats:docs_written(Stats)},
                {<<"doc_write_failures">>, couch_replicator_stats:doc_write_failures(Stats)}
            ]
        end,
        % limit history to 50 entries
        NewRepHistory = {
            BaseHistory ++
            [{<<"history">>, lists:sublist([NewHistoryEntry | OldHistory], 50)}]
        },

        try
            {SrcRevPos, SrcRevId} = update_checkpoint(
                Source, SourceLog#doc{body = NewRepHistory}, source),
            {TgtRevPos, TgtRevId} = update_checkpoint(
                Target, TargetLog#doc{body = NewRepHistory}, target),
            NewState = State#rep_state{
                checkpoint_history = NewRepHistory,
                committed_seq = NewTsSeq,
                source_log = SourceLog#doc{revs={SrcRevPos, [SrcRevId]}},
                target_log = TargetLog#doc{revs={TgtRevPos, [TgtRevId]}}
            },
            {ok, update_job_state(NewState)}
        catch throw:{checkpoint_commit_failure, _} = Failure ->
            Failure
        end;
    {SrcInstanceStartTime, _NewTgtInstanceStartTime} ->
        {checkpoint_commit_failure, <<"Target database out of sync. "
            "Try to increase max_dbs_open at the target's server.">>};
    {_NewSrcInstanceStartTime, TgtInstanceStartTime} ->
        {checkpoint_commit_failure, <<"Source database out of sync. "
            "Try to increase max_dbs_open at the source's server.">>};
    {_NewSrcInstanceStartTime, _NewTgtInstanceStartTime} ->
        {checkpoint_commit_failure, <<"Source and target databases out of "
            "sync. Try to increase max_dbs_open at both servers.">>}
    end.


update_checkpoint(Db, Doc, DbType) ->
    try
        update_checkpoint(Db, Doc)
    catch throw:{checkpoint_commit_failure, Reason} ->
        throw({checkpoint_commit_failure,
            <<"Error updating the ", (to_binary(DbType))/binary,
                " checkpoint document: ", (to_binary(Reason))/binary>>})
    end.


update_checkpoint(Db, #doc{id = LogId, body = LogBody} = Doc) ->
    try
        case couch_replicator_api_wrap:update_doc(Db, Doc, [delay_commit]) of
        {ok, PosRevId} ->
            PosRevId;
        {error, Reason} ->
            throw({checkpoint_commit_failure, Reason})
        end
    catch throw:conflict ->
        case (catch couch_replicator_api_wrap:open_doc(Db, LogId, [ejson_body])) of
        {ok, #doc{body = LogBody, revs = {Pos, [RevId | _]}}} ->
            % This means that we were able to update successfully the
            % checkpoint doc in a previous attempt but we got a connection
            % error (timeout for e.g.) before receiving the success response.
            % Therefore the request was retried and we got a conflict, as the
            % revision we sent is not the current one.
            % We confirm this by verifying the doc body we just got is the same
            % that we have just sent.
            {Pos, RevId};
        _ ->
            throw({checkpoint_commit_failure, conflict})
        end
    end.


commit_to_both(Source, Target) ->
    % commit the src async
    ParentPid = self(),
    SrcCommitPid = spawn_link(
        fun() ->
            Result = (catch couch_replicator_api_wrap:ensure_full_commit(Source)),
            ParentPid ! {self(), Result}
        end),

    % commit tgt sync
    TargetResult = (catch couch_replicator_api_wrap:ensure_full_commit(Target)),

    SourceResult = receive
    {SrcCommitPid, Result} ->
        unlink(SrcCommitPid),
        receive {'EXIT', SrcCommitPid, _} -> ok after 0 -> ok end,
        Result;
    {'EXIT', SrcCommitPid, Reason} ->
        {error, Reason}
    end,
    case TargetResult of
    {ok, TargetStartTime} ->
        case SourceResult of
        {ok, SourceStartTime} ->
            {SourceStartTime, TargetStartTime};
        SourceError ->
            {source_error, SourceError}
        end;
    TargetError ->
        {target_error, TargetError}
    end.


compare_replication_logs(SrcDoc, TgtDoc) ->
    #doc{body={RepRecProps}} = SrcDoc,
    #doc{body={RepRecPropsTgt}} = TgtDoc,
    case get_value(<<"session_id">>, RepRecProps) ==
            get_value(<<"session_id">>, RepRecPropsTgt) of
    true ->
        % if the records have the same session id,
        % then we have a valid replication history
        OldSeqNum = get_value(<<"source_last_seq">>, RepRecProps, ?LOWEST_SEQ),
        OldHistory = get_value(<<"history">>, RepRecProps, []),
        {OldSeqNum, OldHistory};
    false ->
        SourceHistory = get_value(<<"history">>, RepRecProps, []),
        TargetHistory = get_value(<<"history">>, RepRecPropsTgt, []),
        couch_log:notice("Replication records differ. "
                "Scanning histories to find a common ancestor.", []),
        couch_log:debug("Record on source:~p~nRecord on target:~p~n",
                [RepRecProps, RepRecPropsTgt]),
        compare_rep_history(SourceHistory, TargetHistory)
    end.


compare_rep_history(S, T) when S =:= [] orelse T =:= [] ->
    couch_log:notice("no common ancestry -- performing full replication", []),
    {?LOWEST_SEQ, []};
compare_rep_history([{S} | SourceRest], [{T} | TargetRest] = Target) ->
    SourceId = get_value(<<"session_id">>, S),
    case has_session_id(SourceId, Target) of
    true ->
        RecordSeqNum = get_value(<<"recorded_seq">>, S, ?LOWEST_SEQ),
        couch_log:notice("found a common replication record with source_seq ~p",
            [RecordSeqNum]),
        {RecordSeqNum, SourceRest};
    false ->
        TargetId = get_value(<<"session_id">>, T),
        case has_session_id(TargetId, SourceRest) of
        true ->
            RecordSeqNum = get_value(<<"recorded_seq">>, T, ?LOWEST_SEQ),
            couch_log:notice("found a common replication record with source_seq ~p",
                [RecordSeqNum]),
            {RecordSeqNum, TargetRest};
        false ->
            compare_rep_history(SourceRest, TargetRest)
        end
    end.


has_session_id(_SessionId, []) ->
    false;
has_session_id(SessionId, [{Props} | Rest]) ->
    case get_value(<<"session_id">>, Props, nil) of
    SessionId ->
        true;
    _Else ->
        has_session_id(SessionId, Rest)
    end.


get_pending_count(#rep_state{options = Options} = St) ->
    Timeout = get_value(connection_timeout, Options),
    TimeoutMicro = Timeout * 1000,
    case get(pending_count_state) of
        {LastUpdate, PendingCount} ->
            case timer:now_diff(os:timestamp(), LastUpdate) > TimeoutMicro of
                true ->
                    NewPendingCount = get_pending_count_int(St),
                    put(pending_count_state, {os:timestamp(), NewPendingCount}),
                    NewPendingCount;
                false ->
                    PendingCount
            end;
        undefined ->
            NewPendingCount = get_pending_count_int(St),
            put(pending_count_state, {os:timestamp(), NewPendingCount}),
            NewPendingCount
    end.


get_pending_count_int(#rep_state{source = #httpdb{} = Db0}=St) ->
    {_, Seq} = St#rep_state.highest_seq_done,
    Db = Db0#httpdb{retries = 3},
    case (catch couch_replicator_api_wrap:get_pending_count(Db, Seq)) of
    {ok, Pending} ->
        Pending;
    _ ->
        null
    end;
get_pending_count_int(#rep_state{source = Db}=St) ->
    {_, Seq} = St#rep_state.highest_seq_done,
    {ok, Pending} = couch_replicator_api_wrap:get_pending_count(Db, Seq),
    Pending.


update_job_state(#rep_state{} = State) ->
    #rep_state{
        current_through_seq = {_, ThroughSeq},
        highest_seq_done = {_, HighestSeq}
    } = State,
    NewStats = rep_stats(State) ++ [
        {source_seq, HighestSeq},
        {through_seq, ThroughSeq}
    ],
    update_job_stats(undefined, State, NewStats).


update_job_stats(JTx, #rep_state{} = State, NewStats) ->
    #rep_state{job = Job, job_data = JobData} = State,
    JsonStats = couch_replicator_stats:to_json(NewStats),
    JobData1 = JobData#{?REP_STATS => JsonStats},
    {Job1, JobData2} = update_job_data(JTx, Job, JobData1),
    State#rep_state{job := Job1, job_data := JobData2}.


rep_stats(State) ->
    #rep_state{
        committed_seq = {_, CommittedSeq},
        stats = Stats
    } = State,
    [
        {revisions_checked, couch_replicator_stats:missing_checked(Stats)},
        {missing_revisions_found, couch_replicator_stats:missing_found(Stats)},
        {docs_read, couch_replicator_stats:docs_read(Stats)},
        {docs_written, couch_replicator_stats:docs_written(Stats)},
        {changes_pending, get_pending_count(State)},
        {doc_write_failures, couch_replicator_stats:doc_write_failures(Stats)},
        {checkpointed_source_seq, CommittedSeq}
    ].


replication_start_error({unauthorized, DbUri}) ->
    {unauthorized, <<"unauthorized to access or create database ", DbUri/binary>>};
replication_start_error({db_not_found, DbUri}) ->
    {db_not_found, <<"could not open ", DbUri/binary>>};
replication_start_error({http_request_failed, _Method, Url0,
        {error, {error, {conn_failed, {error, nxdomain}}}}}) ->
    Url = ?l2b(couch_util:url_strip_password(Url0)),
    {nxdomain, <<"could not resolve ", Url/binary>>};
replication_start_error({http_request_failed, Method0, Url0,
        {error, {code, Code}}}) when is_integer(Code) ->
    Url = ?l2b(couch_util:url_strip_password(Url0)),
    Method = ?l2b(Method0),
    {http_error_code, Code, <<Method/binary, " ", Url/binary>>};
replication_start_error(Error) ->
    Error.


log_replication_start(#rep_state{} = RepState) ->
    #rep_state{
        id = Id,
        doc_id = DocId,
        db_name = DbName,
        options = Options,
        source_name = Source,
        target_name = Target,
        session_id = Sid,
    } = RepState,
    Workers = get_value(worker_processes, Options),
    BatchSize = get_value(worker_batch_size, Options),
    From = case DbName of
        Name when is_binary(Name) ->
            io_lib:format("from doc ~s:~s", [Name, DocId]);
        _ ->
            "from _replicate endpoint"
    end,
    Msg = "Starting replication ~s (~s -> ~s) ~s worker_procesess:~p"
        " worker_batch_size:~p session_id:~s",
    couch_log:notice(Msg, [Id, Source, Target, From, Workers, BatchSize, Sid]).


check_user_filter(#rep_state{} = State) ->
    #rep_state{
        id = RepId,
        base_id = BaseId,
        job = Job,
        job_data = JobData
    } = State,
    case get_rep_id(undefined, Job, JobData) of
        {RepId, BaseId} ->
            ok;
        {OtherId, _} when is_binary(OtherId) ->
            LogMsg = "~p : Replication id was updated ~p -> ~p",
            couch_log:error(LogMsg, [?MODULE, RepId, OtherId]),
            reschedule_job(JTx, Job, JobData),
            throw(finished)
    end.


hist_append(Type, NowSec, #{} := JobData, Info) when is_integer(NowSec), (
        Type =:= ?HIST_ADDED orelse Type =:= ?HIST_STARTED orelse
        Type =:= ?HIST_PENDING orelse Type =:= ?HIST_CRASHED) ->
    {#?JOB_HISTORY := Hist} = JobData,
    Evt1 = #{?HIST_TYPE => Type, ?HIST_TIMESTAMP => NowSec},
    Evt2 = case Info of
        undefined -> Evt1;
        null -> Evt1#{?HIST_REASON => null};
        <<_/binary>> -> Evt1#{?HIST_REASON => Info}
    end,
    Hist1 = [Evt1 | Hist],
    MaxHistory = config:get_integer("replicator", "max_history",
        ?DEFAULT_MAX_HISTORY),
    Hist2 = lists:sublist(Hist1, MaxHistor),
    JobData#{?JOB_HISTORY := Hist2}.


optimize_rate_limited_job(#{} = Options, <<"max_backoff">>) ->
    OptimizedSettings = #{
        <<"checkpoint_interval">> => 5000,
        <<"worker_processes">> => 2,
        <<"worker_batch_size">> => 100,
        <<"http_connections">> => 2
    },
    maps:merge(Options, OptimizedSettings);

optimize_rate_limited_job(#{} = Options, _Other) ->
    Options.


% Health threshold is the minimum amount of time an unhealthy job should run
% crashing before it is considered to be healthy again. HealtThreashold should
% not be 0 as jobs could start and immediately crash, and it shouldn't be
% infinity, since then  consecutive crashes would accumulate forever even if
% job is back to normal.
health_threshold() ->
    config:get_integer("replicator", "health_threshold",
        ?DEFAULT_HEALTH_THRESHOLD_SEC).

min_backoff_penalty_sec() ->
    config:get_integer("replicator", "min_backoff_pentalty_sec",
        ?DEFAULT_MIN_BACKOFF_PENALTY).


max_backoff_penalty_sec() ->
    config:get_integer("replicator", "max_backoff_penalty_sec",
        ?DEFAULT_MAX_BACKOFF_PENALTY).


accept_jitter() ->
    Jitter = config:get_integer("replicator", "accept_jitter",
        ?ACCEPT_JITTER_DEFAULT),
    couch_rand:uniform(erlang:max(1, Jitter)).


update_docs() ->
    config:get_boolean("replicator", "update_docs", ?DEFAULT_UPDATE_DOCS).



-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").


replication_start_error_test() ->
    ?assertEqual({unauthorized, <<"unauthorized to access or create database"
        " http://x/y">>}, replication_start_error({unauthorized,
        <<"http://x/y">>})),
    ?assertEqual({db_not_found, <<"could not open http://x/y">>},
        replication_start_error({db_not_found, <<"http://x/y">>})),
    ?assertEqual({nxdomain,<<"could not resolve http://x/y">>},
        replication_start_error({http_request_failed, "GET", "http://x/y",
        {error, {error, {conn_failed, {error, nxdomain}}}}})),
    ?assertEqual({http_error_code,503,<<"GET http://x/y">>},
        replication_start_error({http_request_failed, "GET", "http://x/y",
        {error, {code, 503}}})).


scheduler_job_format_status_test() ->
    Source = <<"http://u:p@h1/d1">>,
    Target = <<"http://u:p@h2/d2">>,
    Rep = #rep{
        id = {"base", "+ext"},
        source = couch_replicator_docs:parse_rep_db(Source, #{}, #{}),
        target = couch_replicator_docs:parse_rep_db(Target, #{}, #{}),
        options = [{create_target, true}],
        doc_id = <<"mydoc">>,
        db_name = <<"mydb">>
    },
    State = #rep_state{
        source = Rep#rep.source,
        target = Rep#rep.target,
        session_id = <<"a">>,
        start_seq = <<"1">>,
        source_seq = <<"2">>,
        committed_seq = <<"3">>,
        current_through_seq = <<"4">>,
        highest_seq_done = <<"5">>
    },
    Format = format_status(opts_ignored, [pdict, State]),
    ?assertEqual("http://u:*****@h1/d1/", proplists:get_value(source, Format)),
    ?assertEqual("http://u:*****@h2/d2/", proplists:get_value(target, Format)),
    ?assertEqual({"base", "+ext"}, proplists:get_value(rep_id, Format)),
    ?assertEqual([{create_target, true}], proplists:get_value(options, Format)),
    ?assertEqual(<<"mydoc">>, proplists:get_value(doc_id, Format)),
    ?assertEqual(<<"mydb">>, proplists:get_value(db_name, Format)),
    ?assertEqual(<<"a">>, proplists:get_value(session_id, Format)),
    ?assertEqual(<<"1">>, proplists:get_value(start_seq, Format)),
    ?assertEqual(<<"2">>, proplists:get_value(source_seq, Format)),
    ?assertEqual(<<"3">>, proplists:get_value(committed_seq, Format)),
    ?assertEqual(<<"4">>, proplists:get_value(current_through_seq, Format)),
    ?assertEqual(<<"5">>, proplists:get_value(highest_seq_done, Format)).


-endif.
