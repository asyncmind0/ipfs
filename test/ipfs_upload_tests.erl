-module(ipfs_upload_tests).
-include_lib("eunit/include/eunit.hrl").

binary_payloads_test_() ->
    Payloads = [
        {"empty", <<>>}, {"no final newline", <<"abc">>},
        {"LF", <<"abc\n">>}, {"CRLF", <<"abc\r\n">>},
        {"binary", <<0,1,2,255,0,13,10,128>>},
        {"chunk edge minus one", binary:copy(<<255>>, 1048575)},
        {"exact chunk", binary:copy(<<0>>, 1048576)},
        {"multiple chunks", <<(binary:copy(<<17>>, 2097152))/binary, 255>>}
    ],
    [{Label, {timeout, 20, fun() ->
        with_peer(correct, fun(P, Dir) ->
            Path = filename:join(Dir, "package.bin"),
            ok = file:write_file(Path, Data),
            ?assertMatch({ok, [_]}, ipfs:add(P, {file, Path}, 15000)),
            ?assertEqual([{<<"package.bin">>, <<"application/octet-stream">>, Data}], uploaded())
        end)
    end}} || {Label, Data} <- Payloads].

iodata_upload_test() ->
    with_peer(correct, fun(P, _Dir) ->
        ?assertMatch({ok, [_]}, ipfs:add(P, {data, [<<0>>, [255, <<"x\n">>]], "a.bin"}, 5000)),
        ?assertEqual([{<<"a.bin">>, <<"application/octet-stream">>, <<0,255,"x\n">>}], uploaded())
    end).

directory_contents_test() ->
    with_peer(correct, fun(P, Dir) ->
        Root = filename:join(Dir, "nft"),
        ok = file:make_dir(Root),
        ok = file:make_dir(filename:join(Root, "empty")),
        ok = file:make_dir(filename:join(Root, "sub")),
        ok = file:write_file(filename:join(Root,"installation.json"), <<"{\"test\":true}\n">>),
        ok = file:write_file(filename:join([Root,"sub","damage.deb"]), <<0,255,10>>),
        ok = file:make_symlink("missing-target", filename:join(Root,"link")),
        ?assertMatch({ok, _}, ipfs:add(P, {directory, Root}, 10000)),
        Parts = uploaded(),
        Name = ipfs:upload_name(Root),
        ?assertEqual({<<Name/binary,"/empty">>, <<"application/x-directory">>, <<>>},
            lists:keyfind(<<Name/binary,"/empty">>, 1, Parts)),
        ?assertEqual({<<Name/binary,"/sub/damage.deb">>, <<"application/octet-stream">>, <<0,255,10>>},
            lists:keyfind(<<Name/binary,"/sub/damage.deb">>, 1, Parts)),
        ?assertEqual({<<Name/binary,"/link">>, <<"application/symlink">>, <<"missing-target">>},
            lists:keyfind(<<Name/binary,"/link">>, 1, Parts)),
        Names = [N || {N,_,_} <- Parts],
        ?assertEqual(length(Names), length(lists:usort(Names))),
        ?assertEqual(6, length(Names)),
        Verified = verification_paths([]),
        ?assertEqual(2, length(Verified)),
        ?assert(lists:any(fun(B) -> binary:match(B, <<"/sub/damage.deb">>) =/= nomatch end, Verified))
    end).

encoded_filename_test() ->
    with_peer(correct, fun(P, Dir) ->
        Name = <<"quote\"-percent%2F-space -", 195,169, ".bin">>,
        Path = filename:join(unicode:characters_to_binary(Dir), Name),
        ok = file:write_file(Path, <<"original">>),
        ?assertMatch({ok, [_]}, ipfs:add(P, {file, Path}, 5000)),
        ?assertEqual([{Name, <<"application/octet-stream">>, <<"original">>}], uploaded())
    end).

corruption_rejected_test() ->
    with_peer(corrupt_file, fun(P, _Dir) ->
        {error, {upload_integrity_mismatch, Info}} = ipfs:add(P, {data, <<"abc">>, <<"data.bin">>}, 5000),
        ?assertEqual(3, maps:get(expected_bytes, Info)),
        ?assertEqual(3, maps:get(actual_bytes, Info)),
        ?assertEqual(hex(crypto:hash(sha256, <<"abc">>)), maps:get(expected_sha256, Info)),
        ?assertEqual(hex(crypto:hash(sha256, <<"`bc">>)), maps:get(actual_sha256, Info))
    end).

truncation_rejected_test() ->
    with_peer(truncate_file, fun(P, _) ->
        ?assertMatch({error, {upload_integrity_mismatch, #{expected_bytes := 3, actual_bytes := 2}}},
            ipfs:add(P, {data, <<"abc">>, <<"data.bin">>}, 5000))
    end).

extra_bytes_rejected_test() ->
    with_peer(extra_byte, fun(P, _) ->
        ?assertEqual({error, {upload_response_too_large, 3}},
            ipfs:add(P, {data, <<"abc">>, <<"data.bin">>}, 5000))
    end).

cat_error_test() ->
    with_peer(cat_error, fun(P, _) ->
        ?assertEqual({error, {upload_http_status, 500}}, ipfs:add(P, {data, <<"abc">>, <<"a">>}, 5000))
    end).

stream_errors_test_() ->
    [{atom_to_list(Mode), fun() ->
        with_peer(Mode, fun(P, _) ->
            ?assertEqual({error, upload_stream_error}, ipfs:add(P, {data, <<"abc">>, <<"a">>}, 5000))
        end)
    end} || Mode <- [add_trailer_error, cat_trailer_error]].

successful_trailers_test() ->
    with_peer(cat_trailer_ok, fun(P, _) ->
        ?assertMatch({ok, [_]}, ipfs:add(P, {data, <<"abc">>, <<"a">>}, 5000))
    end).

root_identity_test_() ->
    [{atom_to_list(Mode), fun() ->
        with_peer(Mode, fun(P, _) ->
            ?assertEqual({error, {Reason, <<"a">>}}, ipfs:add(P, {data, <<"abc">>, <<"a">>}, 5000))
        end)
    end} || {Mode, Reason} <- [{missing_root, add_root_missing}, {duplicate_root, add_root_ambiguous}]].

name_validation_test() ->
    ?assertEqual(<<"tmp/nft">>, ipfs:upload_name(<<"/tmp/./nft/">>)),
    ?assertThrow({upload_error, invalid_upload_name}, ipfs:upload_name(<<"../nft">>)),
    ?assertThrow({upload_error, invalid_upload_name}, ipfs:upload_name(<<"x\r\ny">>)).

with_peer(Mode, Fun) ->
    {ok, _} = application:ensure_all_started(gun),
    Base = os:getenv("TMPDIR", "/tmp"),
    Dir = filename:join(Base, "ipfs-upload-" ++ binary_to_list(binary:encode_hex(crypto:strong_rand_bytes(8)))),
    ok = file:make_dir(Dir),
    try
        {Port, Peer} = ipfs_upload_test_server:start(Mode),
        try
            {ok, Client} = ipfs:start_link(#{ip => {127,0,0,1}, port => Port}),
            try Fun(Client, Dir)
            after gen_server:stop(Client, normal, 5000) end
        after ipfs_upload_test_server:stop(Peer) end
    after
        file:del_dir_r(Dir),
        flush_fixture_messages()
    end.

uploaded() ->
    receive {uploaded_parts, Parts} -> Parts after 1000 -> error(no_captured_upload) end.
verification_paths(Acc) ->
    receive {verified_path, P} -> verification_paths([P | Acc]) after 0 -> lists:reverse(Acc) end.
flush_fixture_messages() ->
    receive
        {uploaded_parts, _} -> flush_fixture_messages();
        {verified_path, _} -> flush_fixture_messages()
    after 0 -> ok end.
hex(B) -> string:lowercase(binary:encode_hex(B)).
