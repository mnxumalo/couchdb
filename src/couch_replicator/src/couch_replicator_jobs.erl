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

-module(couch_replicator_jobs).


-export([
    % couch_jobs type timeouts
    set_timeout/0,
    get_timeout/0,

    % Job creation and querying
    new_job/7,
    add_job/3,
    remove_job/2,
    get_job_data/2,
    fold_jobs/3,
    pending_count/1,
    pending_count/2,

    % Job subscription
    wait_for_result/1,

    % Job execution
    accept_jobs/1,
    update_job_data/3,
    finish_job/3,
    reschedule_job/4,

    % (..., ?REPLICATION_IDS) -> JobId handling
    try_update_rep_id/3,
    update_rep_id/3,
    clear_old_rep_id/3,
    get_job_id_by_rep_id/2
]).


-include("couch_replicator.hrl").


-define(REP_JOBS, <<"rep_jobs">>).
-define(REP_JOBS_TIMEOUT_SEC, 61).


% Data model
% ----------
%
% State kept in couch_jobs under the ?REP_JOBS type
%
% Job IDs are defined as:
%   * Replicator DB instance UUID + doc ID for persistent replications
%   * Hash(username|source|target|options) for transient replications
%
% To map replication IDs to couch_job jobs, there is a separate index that
% looks like:
%   (?REPLICATION_IDS, RepId) -> JobId
%

set_timeout() ->
    couch_jobs:set_type_timeout(?REP_JOBS, ?REP_JOBS_TIMEOUT_SEC).


get_timeout() ->
    ?REP_JOBS_TIMEOUT_MSEC.


new_job(#{} = Rep, DbName, DbUUID, DocId, State, StateInfo, DocState) ->
    NowSec = erlang:system_time(second),
    AddedEvent = #{?HIST_TYPE => ?HIST_ADDED, ?HIST_TIMESTAMP => NowSec},
    #{
        ?REP => Rep,
        ?REP_ID => null,
        ?BASE_ID => null,
        ?DB_NAME => DbName,
        ?DB_UUID => DbUUID,
        ?DOC_ID => DocId,
        ?ERROR_COUNT => 0,
        ?REP_STATS => #{},
        ?STATE => State,
        ?STATE_INFO => StateInfo,
        ?DOC_STATE => DocState,
        ?LAST_UPDATED_TIME => NowSec,
        ?LAST_START_TIME => 0,
        ?LAST_ERROR => null,
        ?JOB_HISTORY => [AddedEvent],
        ?CHECKPOINT_HISTORY => []
    }.


add_job(Tx, JobId, JobData) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(Tx), fun(JTx) ->
       case couch_jobs:get_job_data(JTx, ?REP_JOBS, JobId) of
            {ok, #{} = OldData} ->
               ok = remove_job(JTx, JobId, OldData);
            {error, not_found} ->
               ok
       end,
       ok = couch_jobs:add(JTx, ?REP_JOBS, JobData)
    end).


remove_job(Tx, JobId) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(Tx), fun(JTx) ->
        case couch_jobs:get_job_data(JTx, ?REP_JOBS, JobId) of
            {ok, #{} = JobData} ->
                ok = remove_job(Tx, JobId, JobData);
            {error, not_found} ->
                ok
        end
    end).


get_job_data(Tx, JobId) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(Tx), fun(JTx) ->
        couch_jobs:get_job_data(JTx, ?REP_JOBS, UserFun, Acc)
    end).


fold_jobs(Tx, UserFun, Acc) when is_function(UserFun, 4) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(Tx), fun(JTx) ->
        couch_jobs:fold_jobs(JTx, ?REP_JOBS, UserFun, Acc)
    end).


pending_count(Tx) ->
    MaxSchedTime = erlang:system_time(second),
    pending_count(Tx, MaxSchedTime).


pending_count(Tx, MaxSchedTime) when is_integer(MaxSchedTime) ->
    couch_jobs:pending_count(Tx, ?REP_JOBS, #{max_sched_time => Time}).


wait_for_result(JobId) ->
    FinishRes = case couch_jobs:subscribe(?REP_JOBS, JobId) of
        {ok, finished, JobData} ->
            {ok, JobData};
        {ok, SubId, _, _} ->
            case couch_jobs:wait(SubId, finished, infinity) of
                {?REP_JOBS, RepId, finished, JobData} -> {ok, JobData};
                timeout -> timeout
            end;
        {error, Error} ->
            {error, Error}
    end,
    case FinishRes of
       {ok, #{?STATE := ?ST_COMPLETED, ?STATE_INFO := CheckpointHistory}} ->
            {ok, CheckpointHistory};
       {ok, #{?STATE_INFO := Error}} ->
            {error, Error}
       timeout ->
            {error, timeout};
       {error, Error} ->
            {error, Error}
    end.


accept_job(MaxSchedTime) when is_integer(MaxSchedTime) ->
    Opts = #{max_sched_time => MaxSchedTime},
    couch_jobs:accept(?REP_JOBS, Opts).


update_job_data(Tx, #{} = Job, #{} = JobData0) ->
    JobData = JobData0#{?LAST_UPDATED_TIME := erlang:system_time(second)},
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(Tx), fun(JTx) ->
        couch_jobs:update(JTx, Job, JobData)
    end).


finish_job(Tx, #{} = Job, #{} = JobData) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(Tx), fun(JTx) ->
        couch_jobs:finish(JTx, Job, JobData)
    end).


reschedule_job(Tx, #{} = Job, #{} = JobData, Time) is_integer(Time) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(Tx), fun(JTx) ->
         {ok, Job1} = couch_jobs:resubmit(JTx, Job, Time),
         ok = couch_jobs:finish(JTx, Job1, JobData)
    end).


try_update_rep_id(Tx, JobId, RepId) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(Tx), fun(JTx) ->
        #{tx := ErlFdbTx, layer_prefix := LayerPrefix} = JTx,
        Key = erlfdb_tuple:pack({?REPLICATION_IDS, RepId}, LayerPrefix),
        case get_job_id_by_rep_id(JTx, RepId) of
            {error, not_found} ->
                ok = erlfdb:set(ErlFdbTx, Key, JobId);
            {ok, JobId} ->
                ok;
            {ok, OtherJobId}} when is_binary(OtherJobId) ->
                {error, {replication_job_conflict, OtherJobId}}
        end
    end).


update_rep_id(Tx, JobId, RepId) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(Tx), fun(JTx) ->
        #{tx := ErlFdbTx, layer_prefix := LayerPrefix} = JTx,
        Key = erlfdb_tuple:pack({?REPLICATION_IDS, RepId}, LayerPrefix),
        ok = erlfdb:set(ErlFdbTx, Key, JobId)
    end).


clear_old_rep_id(_, _, null) ->
    ok;

clear_old_rep_id(Tx, JobId, RepId) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(Tx), fun(JTx) ->
        #{tx := ErlFdbTx, layer_prefix := LayerPrefix} = JTx,
        Key = erlfdb_tuple:pack({?REPLICATION_IDS, RepId}, LayerPrefix),
        case get_job_id_by_rep_id(JTx, RepId) of
            {error, not_found} ->
                ok = erlfdb:clear(ErlFdbTx, Key);
            {ok, JobId} ->
                ok = erlfdb:clear(ErlFdbTx, Key);
            {ok, OtherJobId}} when is_binary(OtherJobId) ->
                ok
        end
    end).


get_job_id_by_rep_id(Tx, RepId) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(Tx), fun(JTx) ->
        #{tx := ErlFdbTx, layer_prefix := LayerPrefix} = JTx,
        Key = erlfdb_tuple:pack({?REPLICATION_IDS, RepId}, LayerPrefix),
        case erlfdb:wait(erlfdb:get(ErlFdbTx, Key)) of
            not_found ->
                {error, not_found};
            <<_/binary>> = JobId ->
                {ok, JobId}
        end
   end).


% Private functions

remove_job(#{jtx := true} = JTx, JobId, OldJobData)
     #{tx := Tx, layer_prefix := LayerPrefix} = JTx,
     case OldJobData of
        #{?REP_ID := null} ->
            couch_jobs:remove(JTx, ?REP_JOBS, JobId);
        #{?REP_ID := RepId} when is_binary(RepId) ->
            Key = erlfdb_tuple:pack({?REPLICATION_IDS, RepId}, LayerPrefix),
            case erlfdb:wait(erlfdb:get(Tx, Key)) of
                not_found -> ok;
                JobId -> erlfdb:clear(Tx, Key);
                <<_/binary>> -> ok
            end,
            couch_jobs:remove(JTx, ?REP_JOBS, RepId)
    end.
