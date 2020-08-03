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

-module(couch_replicator_doc_processor).

-behaviour(gen_server).

-export([
    add_jobs_from_db/2,
    remove_jobs_from_db/2,
    process_change/2,
    docs/1,
    doc/2,
    doc_lookup/3,
    update_docs/0,
    get_worker_ref/1
]).

%-include_lib("couch/include/couch_db.hrl").
-include("couch_replicator.hrl").
%-include_lib("mem3/include/mem3.hrl").

-import(couch_replicator_utils, [
    get_json_value/2,
    get_json_value/3
]).

-type repstate() :: initializing | error | scheduled.

-define(IS_REPLICATOR_DB(DbName), (DbName =:= ?REP_DB_NAME orelse
    binary_part(DbName, byte_size(DbName), -12) =:= <<"/_replicator">>).


% Process replication doc updates

-spec add_jobs_from_db(binary(), binary())-> ok.
add_jobs_from_db(DbName, DbUUID) when is_binary(DbUUID) ->
    try fabric2_db:open(DbName, [{uuid, DbUUID}]) of
        {ok, Db} ->
            fabric2_fdb:transactional(Db, fun(TxDb) ->
                ok = add_jobs_from_db(TxDb)
            end)
    catch
        error:database_does_not_exist ->
            ok
    end.


-spec remove_jobs_from_db(binary()) -> ok.
remove_jobs_from_db(DbUUID) when is_binary(DbUUID) ->
    FoldFun = fun({JTx, JobId, _, JobData}, ok) ->
        case JobData of
            #{?DB_UUID := DbUUID} ->
                ok = couch_replicator_jobs:remove_job(JTx, JobId);
            #{} ->
                ok
        end
    end,
    couch_replicator_jobs:fold_jobs(undefined, FoldFun, ok).



process_change(_Db, #doc{id = <<?DESIGN_DOC_PREFIX, _/binary>>}) ->
    ok;

process_change(#{} = Db, #doc{deleted = true} = Doc) ->
    DbUUID = fabric2_db:uuid(Db),
    JobId = couch_replicator_ids:job_id(DbUUID, Doc#doc.id),
    couch_replicator_jobs:remove_job(undefined, JobId);

process_change(#{} = Db, #doc{deleted = false} = Doc) ->
    #doc{id = DocId, body = {Props} = Body} = Doc,
    DbName = fabric2_db:name(Db),
    DbUUID = fabric2_db:uuid(Db),
    {Rep, DocState, Error} = try
        Rep0 = couch_replicator_docs:parse_rep_doc(Body),
        DocState0 = get_json_value(?REPLICATION_STATE, Props, null),
        {Rep0, DocState0, null}
    catch
        throw:{bad_rep_doc, Reason} ->
            {null, null, couch_replicator_utils:rep_error_to_binary(Reason)}
    end,
    JobId = couch_replicator_ids:job_id(DbUUID, DocId),
    JobData = case Rep of
        null ->
            couch_relicator_jobs:new_job(Rep. DbName, DbUUID, DocId,
                ?ST_FAILED, Error, null);
        #{} ->
            couch_replicator_jobs:new_job(Rep, DbName, DbUUID, DocId,
                ?ST_PENDING, null, DocState)
    end,
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(Db), fun(JTx) ->
        couch_replicate_jobs:get_job_data(JTx, JobId) of
            {ok, #{?REP := null, ?STATE_INFO := Error}} when Rep =:= null ->
                % Same error as before occurred, don't bother updating the job
                ok;
            {ok, #{?REP := null}} when Rep =:= null ->
                % Error occured but it's a different error so the job is updated
                couch_replicator_jobs:add_job(JTx, JobId, JobData);
            {ok, #{?REP := OldRep}} when is_map(Rep) ->
                case couch_replicator_utils:compare_rep_objects(OldRep, Rep) of
                    true ->
                        % Document was changed but none of the parameters relevent
                        % for the replication job have changed, so make it a no-op
                        ok;
                    false ->
                        couch_replicator_jobs:add_job(JTx, JobId, JobData)
                end;
            {error, not_found} ->
                couch_replicator_jobs:add_job(JTx, JobId, JobData)
        end

    end).


% _scheduler/docs HTTP endpoint helpers

-spec docs([atom()]) -> [{[_]}] | [].
docs(States) ->
    HealthThreshold = couch_replicator_scheduler:health_threshold(),
    ets:foldl(fun(RDoc, Acc) ->
        case ejson_doc(RDoc, HealthThreshold) of
            nil ->
                Acc;  % Could have been deleted if job just completed
            {Props} = EJson ->
                {state, DocState} = lists:keyfind(state, 1, Props),
                case ejson_doc_state_filter(DocState, States) of
                    true ->
                        [EJson | Acc];
                    false ->
                        Acc
                end
        end
    end, [], ?MODULE).


-spec doc(binary(), binary()) -> {ok, {[_]}} | {error, not_found}.
doc(Db, DocId) ->
    HealthThreshold = couch_replicator_scheduler:health_threshold(),
    Res = (catch ets:foldl(fun(RDoc, nil) ->
        {Shard, RDocId} = RDoc#rdoc.id,
        case {mem3:dbname(Shard), RDocId} of
            {Db, DocId} ->
                throw({found, ejson_doc(RDoc, HealthThreshold)});
            {_OtherDb, _OtherDocId} ->
                nil
        end
    end, nil, ?MODULE)),
    case Res of
        {found, DocInfo} ->
            {ok, DocInfo};
        nil ->
            {error, not_found}
    end.


-spec doc_lookup(binary(), binary(), integer()) ->
    {ok, {[_]}} | {error, not_found}.
doc_lookup(Db, DocId, HealthThreshold) ->
    case ets:lookup(?MODULE, {Db, DocId}) of
        [#rdoc{} = RDoc] ->
            {ok, ejson_doc(RDoc, HealthThreshold)};
        [] ->
            {error, not_found}
    end.


-spec ejson_state_info(binary() | nil) -> binary() | null.
ejson_state_info(nil) ->
    null;
ejson_state_info(Info) when is_binary(Info) ->
    Info;
ejson_state_info(Info) ->
    couch_replicator_utils:rep_error_to_binary(Info).


-spec ejson_rep_id(rep_id() | nil) -> binary() | null.
ejson_rep_id(nil) ->
    null;
ejson_rep_id({BaseId, Ext}) ->
    iolist_to_binary([BaseId, Ext]).


-spec ejson_doc(#rdoc{}, non_neg_integer()) -> {[_]} | nil.
ejson_doc(#rdoc{state = scheduled} = RDoc, HealthThreshold) ->
    #rdoc{id = {DbName, DocId}, rid = RepId} = RDoc,
    JobProps = couch_replicator_scheduler:job_summary(RepId, HealthThreshold),
    case JobProps of
        nil ->
            nil;
        [{_, _} | _] ->
            {[
                {doc_id, DocId},
                {database, DbName},
                {id, ejson_rep_id(RepId)},
                {node, node()} | JobProps
            ]}
    end;

ejson_doc(#rdoc{state = RepState} = RDoc, _HealthThreshold) ->
    #rdoc{
       id = {DbName, DocId},
       info = StateInfo,
       rid = RepId,
       errcnt = ErrorCount,
       last_updated = StateTime,
       rep = Rep
    } = RDoc,
    {[
        {doc_id, DocId},
        {database, DbName},
        {id, ejson_rep_id(RepId)},
        {state, RepState},
        {info, ejson_state_info(StateInfo)},
        {error_count, ErrorCount},
        {node, node()},
        {last_updated, couch_replicator_utils:iso8601(StateTime)},
        {start_time, couch_replicator_utils:iso8601(Rep#rep.start_time)}
    ]}.


-spec ejson_doc_state_filter(atom(), [atom()]) -> boolean().
ejson_doc_state_filter(_DocState, []) ->
    true;
ejson_doc_state_filter(State, States) when is_list(States), is_atom(State) ->
    lists:member(State, States).


-spec add_jobs_from_db(#{}) -> ok.
add_jobs_from_db(#{} = TxDb) ->
    FoldFun  = fun
        ({meta, _Meta}, ok) -> {ok, ok};
        (complete, ok) -> {ok, ok};
        ({row, Row}, ok) -> ok = process_change(TxDb, get_doc(TxDb, Row))
    end,
    Opts = [{restart_tx, true}],
    {ok, ok} = fabric2_db:fold_docs(Db, FoldFun, ok, Opts),
    ok.


-spec get_doc(#{}, list()) -> #doc{}.
get_doc(Db, Row) ->
    {_, DocId} = lists:keyfind(id, 1, Row),
    {ok, #doc{deleted = false} = Doc} = fabric2_db:open_doc(TxDb, DocId, []),
    Doc.


-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-define(DB, <<"db">>).
-define(DOC1, <<"doc1">>).
-define(DOC2, <<"doc2">>).
-define(R1, {"1", ""}).
-define(R2, {"2", ""}).


doc_processor_test_() ->
    {
        foreach,
        fun setup/0,
        fun teardown/1,
        [
            t_bad_change(),
            t_regular_change(),
            t_change_with_existing_job(),
            t_deleted_change(),
            t_triggered_change(),
            t_completed_change(),
            t_active_replication_completed(),
            t_error_change(),
            t_failed_change(),
            t_change_for_different_node(),
            t_change_when_cluster_unstable(),
            t_ejson_docs()
        ]
    }.


% Can't parse replication doc, so should write failure state to document.
t_bad_change() ->
    ?_test(begin
        ?assertEqual(acc, db_change(?DB, bad_change(), acc)),
        ?assert(updated_doc_with_failed_state())
    end).


% Regular change, parse to a #rep{} and then add job.
t_regular_change() ->
    ?_test(begin
        mock_existing_jobs_lookup([]),
        ?assertEqual(ok, process_change(?DB, change())),
        ?assert(ets:member(?MODULE, {?DB, ?DOC1})),
        ?assert(started_worker({?DB, ?DOC1}))
    end).


% Regular change, parse to a #rep{} and then add job but there is already
% a running job with same Id found.
t_change_with_existing_job() ->
    ?_test(begin
        mock_existing_jobs_lookup([test_rep(?R2)]),
        ?assertEqual(ok, process_change(?DB, change())),
        ?assert(ets:member(?MODULE, {?DB, ?DOC1})),
        ?assert(started_worker({?DB, ?DOC1}))
    end).


% Change is a deletion, and job is running, so remove job.
t_deleted_change() ->
    ?_test(begin
        mock_existing_jobs_lookup([test_rep(?R2)]),
        ?assertEqual(ok, process_change(?DB, deleted_change())),
        ?assert(removed_job(?R2))
    end).


% Change is in `triggered` state. Remove legacy state and add job.
t_triggered_change() ->
    ?_test(begin
        mock_existing_jobs_lookup([]),
        ?assertEqual(ok, process_change(?DB, change(<<"triggered">>))),
        ?assert(removed_state_fields()),
        ?assert(ets:member(?MODULE, {?DB, ?DOC1})),
        ?assert(started_worker({?DB, ?DOC1}))
    end).


% Change is in `completed` state, so skip over it.
t_completed_change() ->
    ?_test(begin
        ?assertEqual(ok, process_change(?DB, change(<<"completed">>))),
        ?assert(did_not_remove_state_fields()),
        ?assertNot(ets:member(?MODULE, {?DB, ?DOC1})),
        ?assert(did_not_spawn_worker())
    end).


% Completed change comes for what used to be an active job. In this case
% remove entry from doc_processor's ets (because there is no linkage or
% callback mechanism for scheduler to tell doc_processsor a replication just
% completed).
t_active_replication_completed() ->
    ?_test(begin
        mock_existing_jobs_lookup([]),
        ?assertEqual(ok, process_change(?DB, change())),
        ?assert(ets:member(?MODULE, {?DB, ?DOC1})),
        ?assertEqual(ok, process_change(?DB, change(<<"completed">>))),
        ?assert(did_not_remove_state_fields()),
        ?assertNot(ets:member(?MODULE, {?DB, ?DOC1}))
    end).


% Change is in `error` state. Remove legacy state and retry
% running the job. This state was used for transient erorrs which are not
% written to the document anymore.
t_error_change() ->
    ?_test(begin
        mock_existing_jobs_lookup([]),
        ?assertEqual(ok, process_change(?DB, change(<<"error">>))),
        ?assert(removed_state_fields()),
        ?assert(ets:member(?MODULE, {?DB, ?DOC1})),
        ?assert(started_worker({?DB, ?DOC1}))
    end).


% Change is in `failed` state. This is a terminal state and it will not
% be tried again, so skip over it.
t_failed_change() ->
    ?_test(begin
        ?assertEqual(ok, process_change(?DB, change(<<"failed">>))),
        ?assert(did_not_remove_state_fields()),
        ?assertNot(ets:member(?MODULE, {?DB, ?DOC1})),
        ?assert(did_not_spawn_worker())
    end).


% Normal change, but according to cluster ownership algorithm, replication
% belongs to a different node, so this node should skip it.
t_change_for_different_node() ->
   ?_test(begin
        meck:expect(couch_replicator_clustering, owner, 2, different_node),
        ?assertEqual(ok, process_change(?DB, change())),
        ?assert(did_not_spawn_worker())
   end).


% Change handled when cluster is unstable (nodes are added or removed), so
% job is not added. A rescan will be triggered soon and change will be
% evaluated again.
t_change_when_cluster_unstable() ->
   ?_test(begin
       meck:expect(couch_replicator_clustering, owner, 2, unstable),
       ?assertEqual(ok, process_change(?DB, change())),
       ?assert(did_not_spawn_worker())
   end).


% Check if docs/0 function produces expected ejson after adding a job
t_ejson_docs() ->
    ?_test(begin
        mock_existing_jobs_lookup([]),
        ?assertEqual(ok, process_change(?DB, change())),
        ?assert(ets:member(?MODULE, {?DB, ?DOC1})),
        EJsonDocs = docs([]),
        ?assertMatch([{[_|_]}], EJsonDocs),
        [{DocProps}] = EJsonDocs,
        {value, StateTime, DocProps1} = lists:keytake(last_updated, 1,
            DocProps),
        ?assertMatch({last_updated, BinVal1} when is_binary(BinVal1),
            StateTime),
        {value, StartTime, DocProps2} = lists:keytake(start_time, 1, DocProps1),
        ?assertMatch({start_time, BinVal2} when is_binary(BinVal2), StartTime),
        ExpectedProps = [
            {database, ?DB},
            {doc_id, ?DOC1},
            {error_count, 0},
            {id, null},
            {info, null},
            {node, node()},
            {state, initializing}
        ],
        ?assertEqual(ExpectedProps, lists:usort(DocProps2))
    end).


get_worker_ref_test_() ->
    {
        setup,
        fun() ->
            ets:new(?MODULE, [named_table, public, {keypos, #rdoc.id}])
        end,
        fun(_) -> ets:delete(?MODULE) end,
        ?_test(begin
            Id = {<<"db">>, <<"doc">>},
            ?assertEqual(nil, get_worker_ref(Id)),
            ets:insert(?MODULE, #rdoc{id = Id, worker = nil}),
            ?assertEqual(nil, get_worker_ref(Id)),
            Ref = make_ref(),
            ets:insert(?MODULE, #rdoc{id = Id, worker = Ref}),
            ?assertEqual(Ref, get_worker_ref(Id))
        end)
    }.


% Test helper functions


setup() ->
    meck:expect(couch_log, info, 2, ok),
    meck:expect(couch_log, notice, 2, ok),
    meck:expect(couch_log, warning, 2, ok),
    meck:expect(couch_log, error, 2, ok),
    meck:expect(config, get, fun(_, _, Default) -> Default end),
    meck:expect(config, listen_for_changes, 2, ok),
    meck:expect(couch_replicator_clustering, owner, 2, node()),
    meck:expect(couch_replicator_clustering, link_cluster_event_listener, 3,
        ok),
    meck:expect(couch_replicator_doc_processor_worker, spawn_worker, 4, pid),
    meck:expect(couch_replicator_scheduler, remove_job, 1, ok),
    meck:expect(couch_replicator_docs, remove_state_fields, 2, ok),
    meck:expect(couch_replicator_docs, update_failed, 3, ok),
    {ok, Pid} = start_link(),
    Pid.


teardown(Pid) ->
    unlink(Pid),
    exit(Pid, kill),
    meck:unload().


removed_state_fields() ->
    meck:called(couch_replicator_docs, remove_state_fields, [?DB, ?DOC1]).


started_worker(_Id) ->
    1 == meck:num_calls(couch_replicator_doc_processor_worker, spawn_worker, 4).


removed_job(Id) ->
    meck:called(couch_replicator_scheduler, remove_job, [test_rep(Id)]).


did_not_remove_state_fields() ->
    0 == meck:num_calls(couch_replicator_docs, remove_state_fields, '_').


did_not_spawn_worker() ->
    0 == meck:num_calls(couch_replicator_doc_processor_worker, spawn_worker,
        '_').

updated_doc_with_failed_state() ->
    1 == meck:num_calls(couch_replicator_docs, update_failed, '_').


mock_existing_jobs_lookup(ExistingJobs) ->
    meck:expect(couch_replicator_scheduler, find_jobs_by_doc,
        fun(?DB, ?DOC1) -> ExistingJobs end).


test_rep(Id) ->
  #rep{id = Id, start_time = {0, 0, 0}}.


change() ->
    {[
        {?REP_ID, ?DOC1},
        {doc, {[
            {<<"_id">>, ?DOC1},
            {<<"source">>, <<"http://srchost.local/src">>},
            {<<"target">>, <<"http://tgthost.local/tgt">>}
        ]}}
    ]}.


change(State) ->
    {[
        {?REP_ID, ?DOC1},
        {doc, {[
            {<<"_id">>, ?DOC1},
            {<<"source">>, <<"http://srchost.local/src">>},
            {<<"target">>, <<"http://tgthost.local/tgt">>},
            {<<"_replication_state">>, State}
        ]}}
    ]}.


deleted_change() ->
    {[
        {?REP_ID, ?DOC1},
        {<<"deleted">>, true},
        {doc, {[
            {<<"_id">>, ?DOC1},
            {<<"source">>, <<"http://srchost.local/src">>},
            {<<"target">>, <<"http://tgthost.local/tgt">>}
        ]}}
    ]}.


bad_change() ->
    {[
        {?REP_ID, ?DOC2},
        {doc, {[
            {<<"_id">>, ?DOC2},
            {<<"source">>, <<"src">>}
        ]}}
    ]}.

-endif.
