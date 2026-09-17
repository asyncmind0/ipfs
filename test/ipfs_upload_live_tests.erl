%% Opt-in integration against a DISPOSABLE local Kubo daemon. Creates pins.
-module(ipfs_upload_live_tests).
-include_lib("eunit/include/eunit.hrl").

live_kubo_roundtrip_test_() ->
    case os:getenv("IPFS_UPLOAD_LIVE") of
        "1" -> {timeout, 120, fun roundtrip/0};
        _ -> []
    end.

roundtrip() ->
    {ok, _} = application:ensure_all_started(gun),
    Port = list_to_integer(os:getenv("IPFS_UPLOAD_TEST_PORT", "5001")),
    Base = os:getenv("TMPDIR", "/tmp"),
    Dir = filename:join(Base, "ipfs-live-" ++ binary_to_list(binary:encode_hex(crypto:strong_rand_bytes(8)))),
    ok = file:make_dir(Dir),
    try
        Root = filename:join(Dir, "nft"),
        ok = file:make_dir(Root),
        ok = file:make_dir(filename:join(Root,"sub")),
        ok = file:make_dir(filename:join(Root,"emptydir")),
        Data = <<(binary:copy(<<0,255,13,10>>, 300000))/binary, 17,23>>,
        Files = [{"damage.deb",Data}, {"installation.json", <<"{\"test\":true}\n">>},
                 {"sub/empty",<<>>}, {"sub/crlf",<<"abc\r\n">>}, {"sub/no-newline",<<"abc">>},
                 {"sub/quote\"-literal%2F", <<0,10,255>>}],
        [ok = file:write_file(filename:join(Root,N), B) || {N,B} <- Files],
        {ok, Client} = ipfs:start_link(#{ip => {127,0,0,1}, port => Port}),
        try
            {ok, Rows} = ipfs:add(Client, {directory, Root}, 60000),
            RootName = ipfs:upload_name(Root),
            [Cid] = [H || #{<<"Name">> := N, <<"Hash">> := H} <- Rows, N =:= RootName],
            lists:foreach(fun({Name, Expected}) ->
                Target = <<Cid/binary, "/", (unicode:characters_to_binary(Name))/binary>>,
                ?assertEqual(Expected, raw_cat(Port, Target))
            end, Files)
        after gen_server:stop(Client, normal, 5000) end
    after file:del_dir_r(Dir) end.

%% Separate whole-body test reader; deliberately does not reuse the new
%% upload verifier or legacy ipfs:cat's Content-Type-dependent JSON decoder.
raw_cat(Port, Target) ->
    {ok, Conn} = gun:open({127,0,0,1}, Port),
    try
        {ok, _} = gun:await_up(Conn, 5000),
        Ref = gun:post(Conn, <<"/api/v0/cat?", (cow_qs:qs([{<<"arg">>,Target}]))/binary>>,
            [{<<"te">>,<<"trailers">>}, {<<"accept-encoding">>,<<"identity">>}], <<>>),
        case gun:await(Conn, Ref, 10000) of
            {response, fin, 200, _} -> <<>>;
            {response, nofin, 200, _} ->
                case gun:await_body(Conn, Ref, 60000) of
                    {ok, Body} -> Body;
                    {ok, Body, Trailers} ->
                        ?assertEqual(undefined, proplists:get_value(<<"x-stream-error">>, Trailers)),
                        Body;
                    Other -> error({cat_failed, Other})
                end;
            Other -> error({cat_failed, Other})
        end
    after gun:close(Conn) end.
