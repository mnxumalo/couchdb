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

-module(couch_views_red_test).

-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch/include/couch_db.hrl").
-include("couch_views.hrl").


-define(TDEF(A), {atom_to_list(A), fun A/0}).


setup() ->
    test_util:start_couch([
            fabric,
            couch_jobs,
            couch_js,
            couch_views
        ]).


teardown(State) ->
    test_util:stop_couch(State).


map_views_test_() ->
    {
        "Map views",
        {
            setup,
            fun setup/0,
            fun teardown/1,
            [
                ?TDEF(should_reduce),
                ?TDEF(should_reduce_rev),
                ?TDEF(should_reduce_start_key),
                ?TDEF(should_reduce_start_key_rev),
                ?TDEF(should_reduce_end_key),
                ?TDEF(should_reduce_end_key_rev),
                ?TDEF(should_reduce_inclusive_end_false),
                ?TDEF(should_reduce_inclusive_end_false_rev),
                ?TDEF(should_reduce_start_and_end_key),
                ?TDEF(should_reduce_start_and_end_key_rev),
                ?TDEF(should_reduce_empty_range),
                ?TDEF(should_reduce_empty_range_rev)
            ]
        }
    }.


should_reduce() ->
    Result = run_query(<<"baz_count">>, #{}),
    Expect = {ok, [{row, [{key, null}, {value, 10}]}]},
    ?assertEqual(Expect, Result).


should_reduce_rev() ->
    Args = #{
        direction => rev
    },
    Result = run_query(<<"baz_count">>, Args),
    Expect = {ok, [{row, [{key, null}, {value, 10}]}]},
    ?assertEqual(Expect, Result).


should_reduce_start_key() ->
    Args = #{
        start_key => 4
    },
    Result = run_query(<<"baz_count">>, Args),
    Expect = {ok, [{row, [{key, null}, {value, 7}]}]},
    ?assertEqual(Expect, Result).


should_reduce_start_key_rev() ->
    Args = #{
        direction => rev,
        start_key => 4
    },
    Result = run_query(<<"baz_count">>, Args),
    Expect = {ok, [{row, [{key, null}, {value, 4}]}]},
    ?assertEqual(Expect, Result).


should_reduce_end_key() ->
    Args = #{
        end_key => 6
    },
    Result = run_query(<<"baz_count">>, Args),
    Expect = {ok, [{row, [{key, null}, {value, 6}]}]},
    ?assertEqual(Expect, Result).


should_reduce_end_key_rev() ->
    Args = #{
        direction => rev,
        end_key => 6
    },
    Result = run_query(<<"baz_count">>, Args),
    Expect = {ok, [{row, [{key, null}, {value, 5}]}]},
    ?assertEqual(Expect, Result).


should_reduce_inclusive_end_false() ->
    Args = #{
        end_key => 6,
        inclusive_end => false
    },
    Result = run_query(<<"baz_count">>, Args),
    Expect = {ok, [{row, [{key, null}, {value, 5}]}]},
    ?assertEqual(Expect, Result).


should_reduce_inclusive_end_false_rev() ->
    Args = #{
        direction => rev,
        end_key => 6,
        inclusive_end => false
    },
    Result = run_query(<<"baz_count">>, Args),
    Expect = {ok, [{row, [{key, null}, {value, 4}]}]},
    ?assertEqual(Expect, Result).


should_reduce_start_and_end_key() ->
    Args = #{
        start_key => 3,
        end_key => 5
    },
    Result = run_query(<<"baz_count">>, Args),
    Expect = {ok, [{row, [{key, null}, {value, 3}]}]},
    ?assertEqual(Expect, Result).


should_reduce_start_and_end_key_rev() ->
    Args = #{
        direction => rev,
        start_key => 5,
        end_key => 3
    },
    Result = run_query(<<"baz_count">>, Args),
    Expect = {ok, [{row, [{key, null}, {value, 3}]}]},
    ?assertEqual(Expect, Result).


should_reduce_empty_range() ->
    Args = #{
        start_key => 100000,
        end_key => 100001
    },
    Result = run_query(<<"baz_count">>, Args),
    Expect = {ok, [{row, [{key, null}, {value, 0}]}]},
    ?assertEqual(Expect, Result).


should_reduce_empty_range_rev() ->
    Args = #{
        direction => rev,
        start_key => 100001,
        end_key => 100000
    },
    Result = run_query(<<"baz_count">>, Args),
    Expect = {ok, [{row, [{key, null}, {value, 0}]}]},
    ?assertEqual(Expect, Result).


run_query(Idx, Args) ->
    run_query(Idx, Args, false).


run_query(Idx, Args, DebugCluster) ->
    DbName = ?tempdb(),
    {ok, Db} = fabric2_db:create(DbName, [{user_ctx, ?ADMIN_USER}]),
    DDoc = create_ddoc(),
    Docs = make_docs(10),
    fabric2_db:update_docs(Db, [DDoc | Docs]),
    if not DebugCluster -> ok; true ->
        couch_views:query(Db, DDoc, Idx, fun default_cb/2, [], #{}),
        fabric2_fdb:debug_cluster(),
        ok
    end,
    couch_views:query(Db, DDoc, Idx, fun default_cb/2, [], Args).


default_cb(complete, Acc) ->
    {ok, lists:reverse(Acc)};
default_cb({final, Info}, []) ->
    {ok, [Info]};
default_cb({final, _}, Acc) ->
    {ok, Acc};
default_cb({meta, _}, Acc) ->
    {ok, Acc};
default_cb(ok, ddoc_updated) ->
    {ok, ddoc_updated};
default_cb(Row, Acc) ->
    {ok, [Row | Acc]}.


create_ddoc() ->
    couch_doc:from_json_obj({[
        {<<"_id">>, <<"_design/bar">>},
        {<<"views">>, {[
            {<<"baz">>, {[
                {<<"map">>, <<"function(doc) {emit(doc.val, doc.val);}">>}
            ]}},
            {<<"baz_count">>, {[
                {<<"map">>, <<"function(doc) {emit(doc.val, doc.val);}">>},
                {<<"reduce">>, <<"_count">>}
            ]}},
            {<<"baz_size">>, {[
                {<<"map">>, <<"function(doc) {emit(doc.val, doc.val);}">>},
                {<<"reduce">>, <<"_sum">>}
            ]}},
            {<<"boom">>, {[
                {<<"map">>, <<
                    "function(doc) {\n"
                    "   emit([doc.val.toString(), doc.val], doc.val);\n"
                    "}"
                >>},
                {<<"reduce">>, <<"_sum">>}
            ]}},
            {<<"bing">>, {[
                {<<"map">>, <<"function(doc) {}">>},
                {<<"reduce">>, <<"_count">>}
            ]}},
            {<<"bing_hyper">>, {[
                {<<"map">>, <<"function(doc) {}">>},
                {<<"reduce">>, <<"_approx_count_distinct">>}
            ]}},
            {<<"doc_emit">>, {[
                {<<"map">>, <<"function(doc) {emit(doc.val, doc)}">>}
            ]}},
            {<<"duplicate_keys">>, {[
                {<<"map">>, <<
                    "function(doc) {\n"
                    "   emit(doc._id, doc.val);\n"
                    "   emit(doc._id, doc.val + 1);\n"
                    "}">>},
                {<<"reduce">>, <<"_count">>}
            ]}},
            {<<"zing">>, {[
                {<<"map">>, <<
                    "function(doc) {\n"
                    "  if(doc.foo !== undefined)\n"
                    "    emit(doc.foo, 0);\n"
                    "}"
                >>}
            ]}}
        ]}}
    ]}).


make_docs(Count) ->
    [doc(I) || I <- lists:seq(1, Count)].


doc(Id) ->
    couch_doc:from_json_obj({[
        {<<"_id">>, list_to_binary(integer_to_list(Id))},
        {<<"val">>, Id}
    ]}).
