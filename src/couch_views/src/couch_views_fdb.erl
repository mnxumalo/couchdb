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

-module(couch_views_fdb).

-export([
    new_interactive_index/3,
    new_creation_vs/3,
    get_creation_vs/2,
    get_build_status/2,
    set_build_status/3,

    get_update_seq/2,
    set_update_seq/3,

    set_trees/2,

    get_row_count/2,
    get_kv_size/2,

    fold_map_idx/5,

    write_doc/3,

    list_signatures/1,
    clear_index/2
]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-define(LIST_VALUE, 0).
-define(JSON_VALUE, 1).
-define(VALUE, 2).


-include("couch_views.hrl").
-include_lib("couch_mrview/include/couch_mrview.hrl").
-include_lib("fabric/include/fabric2.hrl").


new_interactive_index(Db, Mrst, VS) ->
    couch_views_fdb:new_creation_vs(Db, Mrst, VS),
    couch_views_fdb:set_build_status(Db, Mrst, ?INDEX_BUILDING).


%Interactive View Creation Versionstamp
%(<db>, ?DB_VIEWS, ?VIEW_INFO, ?VIEW_CREATION_VS, Sig) = VS

new_creation_vs(TxDb, #mrst{} = Mrst, VS) ->
    #{
        tx := Tx
    } = TxDb,
    Key = creation_vs_key(TxDb, Mrst#mrst.sig),
    Value = erlfdb_tuple:pack_vs({VS}),
    ok = erlfdb:set_versionstamped_value(Tx, Key, Value).


get_creation_vs(TxDb, #mrst{} = Mrst) ->
    get_creation_vs(TxDb, Mrst#mrst.sig);

get_creation_vs(TxDb, Sig) ->
    #{
        tx := Tx
    } = TxDb,
    Key = creation_vs_key(TxDb, Sig),
    case erlfdb:wait(erlfdb:get(Tx, Key)) of
        not_found ->
            not_found;
        EK ->
            {VS} = erlfdb_tuple:unpack(EK),
            VS
    end.


%Interactive View Build Status
%(<db>, ?DB_VIEWS, ?VIEW_INFO, ?VIEW_BUILD_STATUS, Sig) = INDEX_BUILDING | INDEX_READY

get_build_status(TxDb, #mrst{sig = Sig}) ->
    #{
        tx := Tx
    } = TxDb,
    Key = build_status_key(TxDb, Sig),
    erlfdb:wait(erlfdb:get(Tx, Key)).


set_build_status(TxDb, #mrst{sig = Sig}, State) ->
    #{
        tx := Tx
    } = TxDb,

    Key = build_status_key(TxDb, Sig),
    ok = erlfdb:set(Tx, Key, State).


% View Build Sequence Access
% (<db>, ?DB_VIEWS, Sig, ?VIEW_UPDATE_SEQ) = Sequence


get_update_seq(TxDb, #mrst{sig = Sig}) ->
    #{
        tx := Tx,
        db_prefix := DbPrefix
    } = TxDb,

    case erlfdb:wait(erlfdb:get(Tx, seq_key(DbPrefix, Sig))) of
        not_found -> <<>>;
        UpdateSeq -> UpdateSeq
    end.


set_update_seq(TxDb, Sig, Seq) ->
    #{
        tx := Tx,
        db_prefix := DbPrefix
    } = TxDb,
    ok = erlfdb:set(Tx, seq_key(DbPrefix, Sig), Seq).


set_trees(TxDb, Mrst0) ->
    Mrst1 = open_id_tree(TxDb, Mrst0),
    Views = lists:map(fun(View) ->
        open_view_tree(TxDb, Mrst1#mrst.sig, View)
    end, Mrst1#mrst.views),
    Mrst1#mrst{
        views = Views
    }.


get_row_count(TxDb, View) ->
    #{
        tx := Tx
    } = TxDb,
    {Count, _} = ebtree:full_reduce(Tx, View#mrview.btree),
    Count.


get_kv_size(TxDb, View) ->
    #{
        tx := Tx
    } = TxDb,
    {_, TotalSize} = ebtree:full_reduce(Tx, View#mrview.btree),
    TotalSize.


fold_map_idx(TxDb, View, Options, Callback, Acc0) ->
    #{
        tx := Tx
    } = TxDb,
    #mrview{
        btree = Btree
    } = View,

    CollateFun = collate_fun(View),

    Dir = case lists:keyfind(dir, 1, Options) of
        {dir, D} -> D;
        _ -> fwd
    end,

    InclusiveEnd = case lists:keyfind(inclusive_end, 1, Options) of
        {inclusive_end, IE} -> IE;
        _ -> true
    end,

    StartKey = case lists:keyfind(start_key, 1, Options) of
        {start_key, SK} -> SK;
        false when Dir == fwd -> ebtree:min();
        false when Dir == rev -> ebtree:max()
    end,

    EndKey = case lists:keyfind(end_key, 1, Options) of
        {end_key, EK} -> EK;
        false when Dir == fwd -> ebtree:max();
        false when Dir == rev -> ebtree:min()
    end,

    Wrapper = fun(KVs0, WAcc) ->
        % Remove any keys that match Start or End key
        % depending on direction
        KVs1 = case InclusiveEnd of
            true ->
                KVs0;
            false when Dir == fwd ->
                lists:filter(fun({K, _V}) ->
                    case CollateFun(K, EndKey) of
                        true ->
                            % K =< EndKey
                            case CollateFun(EndKey, K) of
                                true ->
                                    % K == EndKey, so reject
                                    false;
                                false ->
                                    % K < EndKey, so include
                                    true
                            end;
                        false when Dir == fwd ->
                            % K > EndKey, should never happen, but reject
                            false
                    end
                end, KVs0);
            false when Dir == rev ->
                lists:filter(fun({K, _V}) ->
                    % In reverse, if K =< EndKey, we drop it
                    not CollateFun(K, EndKey)
                end, KVs0)
        end,
        % Expand dups
        KVs2 = lists:flatmap(fun({K, V}) ->
            case V of
                {dups, Dups} when Dir == fwd ->
                    [{K, D} || D <- Dups];
                {dups, Dups} when Dir == rev ->
                    [{K, D} || D <- lists:reverse(Dups)];
                _ ->
                    [{K, V}]
            end
        end, KVs1),
        lists:foldl(fun({{Key, DocId}, Value}, WAccInner) ->
            Callback(DocId, Key, Value, WAccInner)
        end, WAcc, KVs2)
    end,

    case Dir of
        fwd ->
            ebtree:range(Tx, Btree, StartKey, EndKey, Wrapper, Acc0);
        rev ->
            % Start/End keys swapped on purpose because ebtree
            ebtree:reverse_range(Tx, Btree, EndKey, StartKey, Wrapper, Acc0)
    end.


fold_red_idx(TxDb, View, Idx, Language, Options, Callback, Acc0) ->
    #{
        tx := Tx
    } = TxDb,
    #mrview{
        btree = Btree
    } = View,

    CollateFun = collate_fun(View),

    Dir = case lists:keyfind(dir, 1, Options) of
        {dir, D} -> D;
        _ -> fwd
    end,

    InclusiveEnd = case lists:keyfind(inclusive_end, 1, Options) of
        {inclusive_end, IE} -> IE;
        _ -> true
    end,

    StartKey = case lists:keyfind(start_key, 1, Options) of
        {start_key, SK} -> SK;
        false when Dir == fwd -> ebtree:min();
        false when Dir == rev -> ebtree:max()
    end,

    EndKey = case lists:keyfind(end_key, 1, Options) of
        {end_key, EK} -> EK;
        false when Dir == fwd -> ebtree:max();
        false when Dir == rev -> ebtree:min()
    end,

    Wrapper = fun(KVs0, WAcc) ->
        % Remove any keys that match Start or End key
        % depending on direction
        KVs1 = case InclusiveEnd of
            true ->
                KVs0;
            false when Dir == fwd ->
                lists:filter(fun({K, _V}) ->
                    case CollateFun(K, EndKey) of
                        true ->
                            % K =< EndKey
                            case CollateFun(EndKey, K) of
                                true ->
                                    % K == EndKey, so reject
                                    false;
                                false ->
                                    % K < EndKey, so include
                                    true
                            end;
                        false when Dir == fwd ->
                            % K > EndKey, should never happen, but reject
                            false
                    end
                end, KVs0);
            false when Dir == rev ->
                lists:filter(fun({K, _V}) ->
                    % In reverse, if K =< EndKey, we drop it
                    not CollateFun(K, EndKey)
                end, KVs0)
        end,
        % Expand dups
        KVs2 = lists:flatmap(fun({K, V}) ->
            case V of
                {dups, Dups} when Dir == fwd ->
                    [{K, D} || D <- Dups];
                {dups, Dups} when Dir == rev ->
                    [{K, D} || D <- lists:reverse(Dups)];
                _ ->
                    [{K, V}]
            end
        end, KVs1),
        lists:foldl(fun({{Key, DocId}, Value}, WAccInner) ->
            Callback(DocId, Key, Value, WAccInner)
        end, WAcc, KVs2)
    end,

    case Dir of
        fwd ->
            ebtree:range(Tx, Btree, StartKey, EndKey, Wrapper, Acc0);
        rev ->
            % Start/End keys swapped on purpose because ebtree
            ebtree:reverse_range(Tx, Btree, EndKey, StartKey, Wrapper, Acc0)
    end.


write_doc(TxDb, Mrst, #{deleted := true} = Doc) ->
    #{
        tx := Tx
    } = TxDb,
    #{
        id := DocId
    } = Doc,

    ExistingViewKeys = get_view_keys(TxDb, Mrst, DocId),

    ebtree:delete(Tx, Mrst#mrst.id_btree, DocId),
    lists:foreach(fun(#mrview{id_num = ViewId, btree = Btree}) ->
        ViewKeys = case lists:keyfind(ViewId, 1, ExistingViewKeys) of
            {ViewId, Keys} -> Keys;
            false -> []
        end,
        lists:foreach(fun(Key) ->
            ebtree:delete(Tx, Btree, {Key, DocId})
        end, ViewKeys)
    end, Mrst#mrst.views);

write_doc(TxDb, Mrst, Doc) ->
    #{
        tx := Tx
    } = TxDb,
    #{
        id := DocId,
        results := Results
    } = Doc,

    ExistingViewKeys = get_view_keys(TxDb, Mrst, DocId),

    NewIdKeys = lists:foldl(fun({View, RawNewRows}, IdKeyAcc) ->
        #mrview{
            id_num = ViewId
        } = View,

        % Remove old keys in the view
        ExistingKeys = case lists:keyfind(ViewId, 1, ExistingViewKeys) of
            {ViewId, Keys} -> Keys;
            false -> []
        end,
        lists:foreach(fun(K) ->
            ebtree:delete(Tx, View#mrview.btree, {K, DocId})
        end, ExistingKeys),

        % Insert new rows
        NewRows = dedupe_rows(View, RawNewRows),
        lists:foreach(fun({K, V}) ->
            ebtree:insert(Tx, View#mrview.btree, {K, DocId}, V)
        end, NewRows),
        ViewKeys = {View#mrview.id_num, lists:usort([K || {K, _V} <- NewRows])},
        [ViewKeys | IdKeyAcc]
    end, [], lists:zip(Mrst#mrst.views, Results)),

    ebtree:insert(Tx, Mrst#mrst.id_btree, DocId, NewIdKeys).


list_signatures(Db) ->
    #{
        db_prefix := DbPrefix
    } = Db,
    ViewSeqRange = {?DB_VIEWS, ?VIEW_INFO, ?VIEW_UPDATE_SEQ},
    RangePrefix = erlfdb_tuple:pack(ViewSeqRange, DbPrefix),
    fabric2_fdb:fold_range(Db, RangePrefix, fun({Key, _Val}, Acc) ->
        {Sig} = erlfdb_tuple:unpack(Key, RangePrefix),
        [Sig | Acc]
    end, [], []).


clear_index(Db, Signature) ->
    #{
        tx := Tx,
        db_prefix := DbPrefix
    } = Db,

    % Clear index info keys
    Keys = [
        {?DB_VIEWS, ?VIEW_INFO, ?VIEW_UPDATE_SEQ, Signature},
        {?DB_VIEWS, ?VIEW_INFO, ?VIEW_ROW_COUNT, Signature},
        {?DB_VIEWS, ?VIEW_INFO, ?VIEW_KV_SIZE, Signature}
    ],
    lists:foreach(fun(Key) ->
        FDBKey = erlfdb_tuple:pack(Key, DbPrefix),
        erlfdb:clear(Tx, FDBKey)
    end, Keys),

    % Clear index data
    DataTuple = {?DB_VIEWS, ?VIEW_DATA, Signature},
    DataPrefix = erlfdb_tuple:pack(DataTuple, DbPrefix),
    erlfdb:clear_range_startswith(Tx, DataPrefix),

    % Clear tree data
    TreeTuple = {?DB_VIEWS, ?VIEW_TREES, Signature},
    TreePrefix = erlfdb_tuple:pack(TreeTuple, DbPrefix),
    erlfdb:clear_range_startswith(Tx, TreePrefix).


get_view_keys(TxDb, Mrst, DocId) ->
    #{
        tx := Tx
    } = TxDb,
    #mrst{
        id_btree = IdTree
    } = Mrst,
    case ebtree:lookup(Tx, IdTree, DocId) of
        {DocId, ViewKeys} -> ViewKeys;
        false -> []
    end.


open_id_tree(TxDb, #mrst{sig = Sig} = Mrst) ->
    #{
        tx := Tx,
        db_prefix := DbPrefix
    } = TxDb,
    Prefix = id_tree_prefix(DbPrefix, Sig),
    Mrst#mrst{
        id_btree = ebtree:open(Tx, Prefix, 10, [])
    }.


open_view_tree(TxDb, Sig, View) ->
    #{
        tx := Tx,
        db_prefix := DbPrefix
    } = TxDb,
    #mrview{
        id_num = ViewId
    } = View,
    Prefix = view_tree_prefix(DbPrefix, Sig, ViewId),
    TreeOpts = [
        {collate_fun, collate_fun(View)},
        {reduce_fun, make_reduce_fun(View)}
    ],
    View#mrview{
        btree = ebtree:open(Tx, Prefix, 10, TreeOpts)
    }.


collate_fun(View) ->
    #mrview{
        options = Options
    } = View,
    case couch_util:get_value(<<"collation">>, Options) of
        <<"raw">> -> fun erlang:'=<'/2;
        _ -> fun collate_rows/2
    end.


collate_rows({KeyA, DocIdA}, {KeyB, DocIdB}) ->
    case couch_ejson_compare:less(KeyA, KeyB) of
        -1 -> lt;
        0 when DocIdA < DocIdB -> lt;
        0 when DocIdA == DocIdB -> eq;
        0 -> gt; % when DocIdA > DocIdB
        1 -> gt
    end.


make_reduce_fun(#mrview{}) ->
    fun
        (KVs, _ReReduce = false) ->
            TotalSize = lists:foldl(fun({K, V}, Acc) ->
                KSize = couch_ejson_size:encoded_size(K),
                VSize = case V of
                    {dups, Dups} ->
                        lists:foldl(fun(D, DAcc) ->
                            DAcc + couch_ejson_size:encoded_size(D)
                        end, 0, Dups);
                    _ ->
                        couch_ejson_size:encoded_size(V)
                end,
                KSize + VSize + Acc
            end, 0, KVs),
            {length(KVs), TotalSize};
        (KRs, _ReReduce = true) ->
            lists:foldl(fun({Count, Size}, {CountAcc, SizeAcc}) ->
                {Count + CountAcc, Size + SizeAcc}
            end, {0, 0}, KRs)
    end.


dedupe_rows(View, KVs0) ->
    CollateFun = collate_fun(View),
    KVs1 = lists:sort(fun({KeyA, _}, {KeyB, _}) ->
        CollateFun({KeyA, <<>>}, {KeyB, <<>>})
    end, lists:sort(KVs0)),
    dedupe_rows_int(CollateFun, KVs1).


dedupe_rows_int(_CollateFun, []) ->
    [];

dedupe_rows_int(_CollateFun, [KV]) ->
    [KV];

dedupe_rows_int(CollateFun, [{K1, V1} | RestKVs]) ->
    RestDeduped = dedupe_rows_int(CollateFun, RestKVs),
    case RestDeduped of
        [{K2, V2} | RestRestDeduped] ->
            Equal = case CollateFun({K1, <<>>}, {K2, <<>>}) of
                true ->
                    case CollateFun({K2, <<>>}, {K1, <<>>}) of
                        true ->
                            true;
                        false ->
                            false
                    end;
                false ->
                    false
            end,
            case Equal of
                true ->
                    [{K1, combine_vals(V1, V2)} | RestRestDeduped];
                false ->
                    [{K1, V1} | RestDeduped]
            end;
        [] ->
            [{K1, V1}]
    end.


combine_vals(V1, {dups, V2}) ->
    {dups, [V1 | V2]};
combine_vals(V1, V2) ->
    {dups, [V1, V2]}.


id_tree_prefix(DbPrefix, Sig) ->
    Key = {?DB_VIEWS, ?VIEW_TREES, Sig, ?VIEW_ID_TREE},
    erlfdb_tuple:pack(Key, DbPrefix).


view_tree_prefix(DbPrefix, Sig, ViewId) ->
    Key = {?DB_VIEWS, ?VIEW_TREES, Sig, ?VIEW_ROW_TREES, ViewId},
    erlfdb_tuple:pack(Key, DbPrefix).


seq_key(DbPrefix, Sig) ->
    Key = {?DB_VIEWS, ?VIEW_INFO, ?VIEW_UPDATE_SEQ, Sig},
    erlfdb_tuple:pack(Key, DbPrefix).


creation_vs_key(Db, Sig) ->
    #{
        db_prefix := DbPrefix
    } = Db,
    Key = {?DB_VIEWS, ?VIEW_INFO, ?VIEW_CREATION_VS, Sig},
    erlfdb_tuple:pack(Key, DbPrefix).


build_status_key(Db, Sig) ->
    #{
        db_prefix := DbPrefix
    } = Db,
    Key = {?DB_VIEWS, ?VIEW_INFO, ?VIEW_BUILD_STATUS, Sig},
    erlfdb_tuple:pack(Key, DbPrefix).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

dedupe_basic_test() ->
    View = #mrview{},
    ?assertEqual([{1, 1}], dedupe_rows(View, [{1, 1}])).

dedupe_simple_test() ->
    View = #mrview{},
    ?assertEqual([{1, {dups, [1, 2]}}], dedupe_rows(View, [{1, 1}, {1, 2}])).

-endif.