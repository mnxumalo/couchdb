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

-module(couchdb_access_tests).

-include_lib("couch/include/couch_eunit.hrl").

-define(CONTENT_JSON, {"Content-Type", "application/json"}).
-define(ADMIN_REQ_HEADERS, [?CONTENT_JSON, {basic_auth, {"a", "a"}}]).
-define(USERX_REQ_HEADERS, [?CONTENT_JSON, {basic_auth, {"x", "x"}}]).
-define(USERY_REQ_HEADERS, [?CONTENT_JSON, {basic_auth, {"y", "y"}}]).
-define(SECURITY_OBJECT, {[
 {<<"members">>,{[{<<"roles">>,[<<"_admin">>, <<"_users">>]}]}},
 {<<"admins">>, {[{<<"roles">>,[<<"_admin">>]}]}}
]}).

url() ->
    Addr = config:get("httpd", "bind_address", "127.0.0.1"),
    lists:concat(["http://", Addr, ":", port()]).

before_each(_) ->
    {ok, 201, _, _} = test_request:put(url() ++ "/db?q=1&n=1&access=true", ?ADMIN_REQ_HEADERS, ""),
    {ok, _, _, _} = test_request:put(url() ++ "/db/_security", ?ADMIN_REQ_HEADERS, jiffy:encode(?SECURITY_OBJECT)),
    url().

after_each(_, Url) ->
    {ok, 200, _, _} = test_request:delete(Url ++ "/db", ?ADMIN_REQ_HEADERS),
    ok.

before_all() ->
    Couch = test_util:start_couch([chttpd]),
    Hashed = couch_passwords:hash_admin_password("a"),
    ok = config:set("admins", "a", binary_to_list(Hashed), _Persist=false),
    % ok = config:set("log", "level", "debug", _Persist=false),

    % cleanup and setup
    {ok, _, _, _} = test_request:delete(url() ++ "/db", ?ADMIN_REQ_HEADERS),
    % {ok, _, _, _} = test_request:put(url() ++ "/db?q=1&n=1&access=true", ?ADMIN_REQ_HEADERS, ""),

    % create users
    UserDbUrl = url() ++ "/_users?q=1&n=1",
    {ok, _, _, _} = test_request:delete(UserDbUrl, ?ADMIN_REQ_HEADERS, ""),
    {ok, 201, _, _} = test_request:put(UserDbUrl, ?ADMIN_REQ_HEADERS, ""),

    UserXDocUrl = url() ++ "/_users/org.couchdb.user:x",
    UserXDocBody = "{ \"name\":\"x\", \"roles\": [], \"password\":\"x\", \"type\": \"user\" }",
    {ok, 201, _, _} = test_request:put(UserXDocUrl, ?ADMIN_REQ_HEADERS, UserXDocBody),

    UserYDocUrl = url() ++ "/_users/org.couchdb.user:y",
    UserYDocBody = "{ \"name\":\"y\", \"roles\": [], \"password\":\"y\", \"type\": \"user\" }",
    {ok, 201, _, _} = test_request:put(UserYDocUrl, ?ADMIN_REQ_HEADERS, UserYDocBody),
    Couch.

after_all(_) ->
    ok = test_util:stop_couch(done).

access_test_() ->
    Tests = [
        % Doc creation
        fun should_not_let_anonymous_user_create_doc/2,
        fun should_let_admin_create_doc_with_access/2,
        fun should_let_admin_create_doc_without_access/2,
        fun should_let_user_create_doc_for_themselves/2,
        fun should_not_let_user_create_doc_for_someone_else/2,
        fun should_let_user_create_access_ddoc/2,
        fun access_ddoc_should_have_no_effects/2,

        % Doc updates
        fun users_with_access_can_update_doc/2,
        fun users_with_access_can_not_change_access/2,
        fun users_with_access_can_not_remove_access/2,

        % Doc reads
        fun should_let_admin_read_doc_with_access/2,
        fun user_with_access_can_read_doc/2,
        fun user_without_access_can_not_read_doc/2,
        fun user_can_not_read_doc_without_access/2,
        fun admin_with_access_can_read_conflicted_doc/2,
        fun user_with_access_can_not_read_conflicted_doc/2,

        % Doc deletes
        fun should_let_admin_delete_doc_with_access/2,
        fun should_let_user_delete_doc_for_themselves/2,
        fun should_not_let_user_delete_doc_for_someone_else/2,

        % _all_docs with include_docs
        fun should_let_admin_fetch_all_docs/2,
        fun should_let_user_fetch_their_own_all_docs/2,
        % potential future feature
        % % % fun should_let_user_fetch_their_own_all_docs_plus_users_ddocs/2%,

        % _changes
        fun should_let_admin_fetch_changes/2,
        fun should_let_user_fetch_their_own_changes/2,

        % views
        fun should_not_allow_admin_access_ddoc_view_request/2,
        fun should_not_allow_user_access_ddoc_view_request/2,
        fun should_allow_admin_users_access_ddoc_view_request/2,
        fun should_allow_user_users_access_ddoc_view_request/2

        % TODO: create test db with role and not _users in _security.members
        % and make sure a user in that group can access while a user not
        % in that group cant
    ],
    {
        "Access tests",
        {
            setup,
            fun before_all/0, fun after_all/1,
            [
                make_test_cases(clustered, Tests)
            ]
        }
    }.

make_test_cases(Mod, Funs) ->
    {
        lists:flatten(io_lib:format("~s", [Mod])),
        {foreachx, fun before_each/1, fun after_each/2, [{Mod, Fun} || Fun <- Funs]}
    }.

% Doc creation

should_not_let_anonymous_user_create_doc(_PortType, Url) ->
    {ok, Code, _, _} = test_request:put(Url ++ "/db/a", "{\"a\":1,\"_access\":[\"x\"]}"),
    ?_assertEqual(401, Code).

should_let_admin_create_doc_with_access(_PortType, Url) ->
    {ok, Code, _, _} = test_request:put(Url ++ "/db/a",
        ?ADMIN_REQ_HEADERS, "{\"a\":1,\"_access\":[\"x\"]}"),
    ?_assertEqual(201, Code).

should_let_admin_create_doc_without_access(_PortType, Url) ->
    {ok, Code, _, _} = test_request:put(Url ++ "/db/a",
        ?ADMIN_REQ_HEADERS, "{\"a\":1}"),
    ?_assertEqual(201, Code).

should_let_user_create_doc_for_themselves(_PortType, Url) ->
    {ok, Code, _, _} = test_request:put(Url ++ "/db/b",
        ?USERX_REQ_HEADERS, "{\"a\":1,\"_access\":[\"x\"]}"),
    ?_assertEqual(201, Code).

should_not_let_user_create_doc_for_someone_else(_PortType, Url) ->
    {ok, Code, _, _} = test_request:put(Url ++ "/db/c",
        ?USERY_REQ_HEADERS, "{\"a\":1,\"_access\":[\"x\"]}"),
    ?_assertEqual(403, Code).

should_let_user_create_access_ddoc(_PortType, Url) ->
    {ok, Code, _, _} = test_request:put(Url ++ "/db/_design/dx",
        ?USERX_REQ_HEADERS, "{\"a\":1,\"_access\":[\"x\"]}"),
    ?_assertEqual(201, Code).

access_ddoc_should_have_no_effects(_PortType, Url) ->
    Ddoc = "{ \"_access\":[\"x\"], \"validate_doc_update\": \"function(newDoc, oldDoc, userCtx) { throw({unauthorized: 'throw error'})}\",   \"views\": {     \"foo\": {       \"map\": \"function(doc) { emit(doc._id) }\"     }   },   \"shows\": {     \"boo\": \"function() {}\"   },   \"lists\": {     \"hoo\": \"function() {}\"   },   \"update\": {     \"goo\": \"function() {}\"   },   \"filters\": {     \"loo\": \"function() {}\"   }   }",
    {ok, Code, _, _} = test_request:put(Url ++ "/db/_design/dx",
        ?USERX_REQ_HEADERS, Ddoc),
    ?_assertEqual(201, Code),
    {ok, Code1, _, _} = test_request:put(Url ++ "/db/b",
        ?USERX_REQ_HEADERS, "{\"a\":1,\"_access\":[\"x\"]}"),
    ?_assertEqual(401, Code1),
    {ok, Code2, _, _} = test_request:get(Url ++ "/db/_design/dx/_view/foo",
        ?USERX_REQ_HEADERS),
    ?_assertEqual(403, Code2),
    {ok, Code3, _, _} = test_request:get(Url ++ "/db/_design/dx/_show/boo/b",
        ?USERX_REQ_HEADERS),
    ?_assertEqual(403, Code3),
    {ok, Code4, _, _} = test_request:get(Url ++ "/db/_design/dx/_list/hoo/foo",
        ?USERX_REQ_HEADERS),
    ?_assertEqual(403, Code4),
    {ok, Code5, _, _} = test_request:post(Url ++ "/db/_design/dx/_update/goo",
        ?USERX_REQ_HEADERS, ""),
    ?_assertEqual(403, Code5),
    {ok, Code6, _, _} = test_request:get(Url ++ "/db/_changes?filter=dx/loo",
        ?USERX_REQ_HEADERS),
    ?_assertEqual(403, Code6),
    {ok, Code7, _, _} = test_request:get(Url ++ "/db/_changes?filter=_view&view=dx/foo",
        ?USERX_REQ_HEADERS),
    ?_assertEqual(403, Code7).

% Doc updates

users_with_access_can_update_doc(_PortType, Url) ->
    {ok, _, _, Body} = test_request:put(Url ++ "/db/b",
        ?USERX_REQ_HEADERS, "{\"a\":1,\"_access\":[\"x\"]}"),
    {Json} = jiffy:decode(Body),
    Rev = couch_util:get_value(<<"rev">>, Json),
    {ok, Code, _, _} = test_request:put(Url ++ "/db/b",
        ?USERX_REQ_HEADERS,
        "{\"a\":2,\"_access\":[\"x\"],\"_rev\":\"" ++ binary_to_list(Rev) ++ "\"}"),
    ?_assertEqual(201, Code).

users_with_access_can_not_change_access(_PortType, Url) ->
    {ok, _, _, Body} = test_request:put(Url ++ "/db/b",
        ?USERX_REQ_HEADERS, "{\"a\":1,\"_access\":[\"x\"]}"),
    {Json} = jiffy:decode(Body),
    Rev = couch_util:get_value(<<"rev">>, Json),
    {ok, Code, _, _} = test_request:put(Url ++ "/db/b",
        ?USERX_REQ_HEADERS,
        "{\"a\":2,\"_access\":[\"y\"],\"_rev\":\"" ++ binary_to_list(Rev) ++ "\"}"),
    ?_assertEqual(403, Code).

users_with_access_can_not_remove_access(_PortType, Url) ->
    {ok, _, _, Body} = test_request:put(Url ++ "/db/b",
        ?USERX_REQ_HEADERS, "{\"a\":1,\"_access\":[\"x\"]}"),
    {Json} = jiffy:decode(Body),
    Rev = couch_util:get_value(<<"rev">>, Json),
    {ok, Code, _, _} = test_request:put(Url ++ "/db/b",
        ?USERX_REQ_HEADERS,
        "{\"a\":2,\"_rev\":\"" ++ binary_to_list(Rev) ++ "\"}"),
    ?_assertEqual(403, Code).

% Doc reads
should_let_admin_read_doc_with_access(_PortType, Url) ->
    {ok, 201, _, _} = test_request:put(Url ++ "/db/a",
        ?USERX_REQ_HEADERS, "{\"a\":1,\"_access\":[\"x\"]}"),
    {ok, Code, _, _} = test_request:get(Url ++ "/db/a",
        ?ADMIN_REQ_HEADERS),
    ?_assertEqual(200, Code).

user_with_access_can_read_doc(_PortType, Url) ->
    {ok, 201, _, _} = test_request:put(Url ++ "/db/a",
        ?ADMIN_REQ_HEADERS, "{\"a\":1,\"_access\":[\"x\"]}"),
    {ok, Code, _, _} = test_request:get(Url ++ "/db/a",
        ?USERX_REQ_HEADERS),
    ?_assertEqual(200, Code).

user_with_access_can_not_read_conflicted_doc(_PortType, Url) ->
    {ok, 201, _, _} = test_request:put(Url ++ "/db/a",
        ?ADMIN_REQ_HEADERS, "{\"_id\":\"f1\",\"a\":1,\"_access\":[\"x\"]}"),
    {ok, 201, _, _} = test_request:put(Url ++ "/db/a?new_edits=false",
        ?ADMIN_REQ_HEADERS, "{\"_id\":\"f1\",\"_rev\":\"7-XYZ\",\"a\":1,\"_access\":[\"x\"]}"),
    {ok, Code, _, _} = test_request:get(Url ++ "/db/a",
        ?USERX_REQ_HEADERS),
    ?_assertEqual(403, Code).

admin_with_access_can_read_conflicted_doc(_PortType, Url) ->
    {ok, 201, _, _} = test_request:put(Url ++ "/db/a",
        ?ADMIN_REQ_HEADERS, "{\"_id\":\"a\",\"a\":1,\"_access\":[\"x\"]}"),
    {ok, 201, _, _} = test_request:put(Url ++ "/db/a?new_edits=false",
        ?ADMIN_REQ_HEADERS, "{\"_id\":\"a\",\"_rev\":\"7-XYZ\",\"a\":1,\"_access\":[\"x\"]}"),
    {ok, Code, _, _} = test_request:get(Url ++ "/db/a",
        ?ADMIN_REQ_HEADERS),
    ?_assertEqual(200, Code).

user_without_access_can_not_read_doc(_PortType, Url) ->
    {ok, 201, _, _} = test_request:put(Url ++ "/db/a",
        ?ADMIN_REQ_HEADERS, "{\"a\":1,\"_access\":[\"x\"]}"),
    {ok, Code, _, _} = test_request:get(Url ++ "/db/a",
        ?USERY_REQ_HEADERS),
    ?_assertEqual(403, Code).

user_can_not_read_doc_without_access(_PortType, Url) ->
    {ok, 201, _, _} = test_request:put(Url ++ "/db/a",
        ?ADMIN_REQ_HEADERS, "{\"a\":1}"),
    {ok, Code, _, _} = test_request:get(Url ++ "/db/a",
        ?USERX_REQ_HEADERS),
    ?_assertEqual(403, Code).

% Doc deletes
should_let_admin_delete_doc_with_access(_PortType, Url) ->
    {ok, 201, _, _} = test_request:put(Url ++ "/db/a",
        ?USERX_REQ_HEADERS, "{\"a\":1,\"_access\":[\"x\"]}"),
    {ok, Code, _, _} = test_request:delete(Url ++ "/db/a?rev=1-23202479633c2b380f79507a776743d5",
        ?ADMIN_REQ_HEADERS),
    ?_assertEqual(201, Code).

should_let_user_delete_doc_for_themselves(_PortType, Url) ->
    {ok, 201, _, _} = test_request:put(Url ++ "/db/a",
        ?USERX_REQ_HEADERS, "{\"a\":1,\"_access\":[\"x\"]}"),
    {ok, _, _, _} = test_request:get(Url ++ "/db/a",
        ?USERX_REQ_HEADERS),
    {ok, Code, _, _} = test_request:delete(Url ++ "/db/a?rev=1-23202479633c2b380f79507a776743d5",
        ?USERX_REQ_HEADERS),
    ?_assertEqual(201, Code).

should_not_let_user_delete_doc_for_someone_else(_PortType, Url) ->
    {ok, 201, _, _} = test_request:put(Url ++ "/db/a",
        ?USERX_REQ_HEADERS, "{\"a\":1,\"_access\":[\"x\"]}"),
    {ok, Code, _, _} = test_request:delete(Url ++ "/db/a?rev=1-23202479633c2b380f79507a776743d5",
        ?USERY_REQ_HEADERS),
    ?_assertEqual(403, Code).

% _all_docs with include_docs
should_let_admin_fetch_all_docs(_PortType, Url) ->
    {ok, 201, _, _} = test_request:put(Url ++ "/db/a",
        ?ADMIN_REQ_HEADERS, "{\"a\":1,\"_access\":[\"x\"]}"),
    {ok, 201, _, _} = test_request:put(Url ++ "/db/b",
        ?ADMIN_REQ_HEADERS, "{\"b\":2,\"_access\":[\"x\"]}"),
    {ok, 201, _, _} = test_request:put(Url ++ "/db/c",
        ?ADMIN_REQ_HEADERS, "{\"c\":3,\"_access\":[\"y\"]}"),
    {ok, 201, _, _} = test_request:put(Url ++ "/db/d",
        ?ADMIN_REQ_HEADERS, "{\"d\":4,\"_access\":[\"y\"]}"),
    {ok, 200, _, Body} = test_request:get(Url ++ "/db/_all_docs?include_docs=true",
        ?ADMIN_REQ_HEADERS),
    {Json} = jiffy:decode(Body),
    ?_assertEqual(4, proplists:get_value(<<"total_rows">>, Json)).

should_let_user_fetch_their_own_all_docs(_PortType, Url) ->
    {ok, 201, _, _} = test_request:put(Url ++ "/db/a",
        ?ADMIN_REQ_HEADERS, "{\"a\":1,\"_access\":[\"x\"]}"),
    {ok, 201, _, _} = test_request:put(Url ++ "/db/b",
        ?USERX_REQ_HEADERS, "{\"b\":2,\"_access\":[\"x\"]}"),
    {ok, 201, _, _} = test_request:put(Url ++ "/db/c",
        ?ADMIN_REQ_HEADERS, "{\"c\":3,\"_access\":[\"y\"]}"),
    {ok, 201, _, _} = test_request:put(Url ++ "/db/d",
        ?USERY_REQ_HEADERS, "{\"d\":4,\"_access\":[\"y\"]}"),
    {ok, 200, _, Body} = test_request:get(Url ++ "/db/_all_docs?include_docs=true",
        ?USERX_REQ_HEADERS),
    {Json} = jiffy:decode(Body),
    ?_assertEqual(2, length(proplists:get_value(<<"rows">>, Json))).
    % TODO    ?_assertEqual(2, proplists:get_value(<<"total_rows">>, Json)).

% Potential future feature:%
% should_let_user_fetch_their_own_all_docs_plus_users_ddocs(_PortType, Url) ->
%     {ok, 201, _, _} = test_request:put(Url ++ "/db/a",
%         ?ADMIN_REQ_HEADERS, "{\"a\":1,\"_access\":[\"x\"]}"),
%     {ok, 201, _, _} = test_request:put(Url ++ "/db/_design/foo",
%         ?ADMIN_REQ_HEADERS, "{\"a\":1,\"_access\":[\"_users\"]}"),
%     {ok, 201, _, _} = test_request:put(Url ++ "/db/_design/bar",
%         ?ADMIN_REQ_HEADERS, "{\"a\":1,\"_access\":[\"houdini\"]}"),
%     {ok, 201, _, _} = test_request:put(Url ++ "/db/b",
%         ?USERX_REQ_HEADERS, "{\"b\":2,\"_access\":[\"x\"]}"),
%
%     % % TODO: add allowing non-admin users adding non-admin ddocs
%     {ok, 201, _, _} = test_request:put(Url ++ "/db/_design/x",
%         ?ADMIN_REQ_HEADERS, "{\"b\":2,\"_access\":[\"x\"]}"),
%
%     {ok, 201, _, _} = test_request:put(Url ++ "/db/c",
%         ?ADMIN_REQ_HEADERS, "{\"c\":3,\"_access\":[\"y\"]}"),
%     {ok, 201, _, _} = test_request:put(Url ++ "/db/d",
%         ?USERY_REQ_HEADERS, "{\"d\":4,\"_access\":[\"y\"]}"),
%     {ok, 200, _, Body} = test_request:get(Url ++ "/db/_all_docs?include_docs=true",
%         ?USERX_REQ_HEADERS),
%     {Json} = jiffy:decode(Body),
%     ?debugFmt("~nHSOIN: ~p~n", [Json]),
%     ?_assertEqual(3, length(proplists:get_value(<<"rows">>, Json))).

% _changes
should_let_admin_fetch_changes(_PortType, Url) ->
    {ok, 201, _, _} = test_request:put(Url ++ "/db/a",
        ?ADMIN_REQ_HEADERS, "{\"a\":1,\"_access\":[\"x\"]}"),
    {ok, 201, _, _} = test_request:put(Url ++ "/db/b",
        ?ADMIN_REQ_HEADERS, "{\"b\":2,\"_access\":[\"x\"]}"),
    {ok, 201, _, _} = test_request:put(Url ++ "/db/c",
        ?ADMIN_REQ_HEADERS, "{\"c\":3,\"_access\":[\"y\"]}"),
    {ok, 201, _, _} = test_request:put(Url ++ "/db/d",
        ?ADMIN_REQ_HEADERS, "{\"d\":4,\"_access\":[\"y\"]}"),
    {ok, 200, _, Body} = test_request:get(Url ++ "/db/_changes",
        ?ADMIN_REQ_HEADERS),
    {Json} = jiffy:decode(Body),
    AmountOfDocs = length(proplists:get_value(<<"results">>, Json)),
    ?_assertEqual(4, AmountOfDocs).

should_let_user_fetch_their_own_changes(_PortType, Url) ->
    {ok, 201, _, _} = test_request:put(Url ++ "/db/a",
        ?ADMIN_REQ_HEADERS, "{\"a\":1,\"_access\":[\"x\"]}"),
    {ok, 201, _, _} = test_request:put(Url ++ "/db/b",
        ?ADMIN_REQ_HEADERS, "{\"b\":2,\"_access\":[\"x\"]}"),
    {ok, 201, _, _} = test_request:put(Url ++ "/db/c",
        ?ADMIN_REQ_HEADERS, "{\"c\":3,\"_access\":[\"y\"]}"),
    {ok, 201, _, _} = test_request:put(Url ++ "/db/d",
        ?ADMIN_REQ_HEADERS, "{\"d\":4,\"_access\":[\"y\"]}"),
    {ok, 200, _, Body} = test_request:get(Url ++ "/db/_changes",
        ?USERX_REQ_HEADERS),
    {Json} = jiffy:decode(Body),
    AmountOfDocs = length(proplists:get_value(<<"results">>, Json)),
    ?_assertEqual(2, AmountOfDocs).

% views
should_not_allow_admin_access_ddoc_view_request(_PortType, Url) ->
    DDoc = "{\"a\":1,\"_access\":[\"x\"],\"views\":{\"foo\":{\"map\":\"function() {}\"}}}",
    {ok, Code, _, _} = test_request:put(Url ++ "/db/_design/a",
        ?ADMIN_REQ_HEADERS, DDoc),
    ?assertEqual(201, Code),
    {ok, Code1, _, _} = test_request:get(Url ++ "/db/_design/a/_view/foo",
        ?ADMIN_REQ_HEADERS),
    ?_assertEqual(403, Code1).

should_not_allow_user_access_ddoc_view_request(_PortType, Url) ->
    DDoc = "{\"a\":1,\"_access\":[\"x\"],\"views\":{\"foo\":{\"map\":\"function() {}\"}}}",
    {ok, Code, _, _} = test_request:put(Url ++ "/db/_design/a",
        ?ADMIN_REQ_HEADERS, DDoc),
    ?assertEqual(201, Code),
    {ok, Code1, _, _} = test_request:get(Url ++ "/db/_design/a/_view/foo",
        ?USERX_REQ_HEADERS),
    ?_assertEqual(403, Code1).

should_allow_admin_users_access_ddoc_view_request(_PortType, Url) ->
    DDoc = "{\"a\":1,\"_access\":[\"_users\"],\"views\":{\"foo\":{\"map\":\"function() {}\"}}}",
    {ok, Code, _, _} = test_request:put(Url ++ "/db/_design/a",
        ?ADMIN_REQ_HEADERS, DDoc),
    ?assertEqual(201, Code),
    {ok, Code1, _, _} = test_request:get(Url ++ "/db/_design/a/_view/foo",
        ?ADMIN_REQ_HEADERS),
    ?_assertEqual(200, Code1).

should_allow_user_users_access_ddoc_view_request(_PortType, Url) ->
    DDoc = "{\"a\":1,\"_access\":[\"_users\"],\"views\":{\"foo\":{\"map\":\"function() {}\"}}}",
    {ok, Code, _, _} = test_request:put(Url ++ "/db/_design/a",
        ?ADMIN_REQ_HEADERS, DDoc),
    ?assertEqual(201, Code),
    {ok, Code1, _, _} = test_request:get(Url ++ "/db/_design/a/_view/foo",
        ?USERX_REQ_HEADERS),
    ?_assertEqual(200, Code1).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

port() ->
    integer_to_list(mochiweb_socket_server:get(chttpd, port)).