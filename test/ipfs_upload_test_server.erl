%% Loopback-only Kubo stand-in. No production module or code path is replaced.
-module(ipfs_upload_test_server).
-export([start/1, stop/1]).

start(Mode) ->
    {ok, Listen} = gen_tcp:listen(0, [binary, {active, false}, {packet, raw},
                                     {ip, {127,0,0,1}}, {reuseaddr, true}]),
    {ok, {_, Port}} = inet:sockname(Listen),
    Parent = self(),
    {Pid, Mon} = spawn_monitor(fun() ->
        case gen_tcp:accept(Listen) of
            {ok, Sock} ->
                try loop(Sock, <<>>, Parent, Mode, undefined)
                catch exit:normal -> ok
                after gen_tcp:close(Sock) end;
            {error, closed} -> ok
        end
    end),
    {Port, {Listen, Pid, Mon}}.

stop({Listen, Pid, Mon}) ->
    exit(Pid, kill),
    receive {'DOWN', Mon, process, Pid, _} -> ok after 5000 -> error(peer_not_stopped) end,
    gen_tcp:close(Listen).

loop(Sock, Buffer, Parent, Mode, Stored) ->
    {Head, B1} = until(Sock, Buffer, <<"\r\n\r\n">>),
    [RequestLine | HeaderLines] = binary:split(Head, <<"\r\n">>, [global]),
    [<<"POST">>, Uri, _] = binary:split(RequestLine, <<" ">>, [global]),
    H = [header(Line) || Line <- HeaderLines],
    {Body, B2} = case proplists:get_value(<<"transfer-encoding">>, H) of
        <<"chunked">> -> chunks(Sock, B1, []);
        _ -> take(Sock, B1, binary_to_integer(proplists:get_value(<<"content-length">>, H, <<"0">>)))
    end,
    U = uri_string:parse(Uri),
    Path = maps:get(path, U),
    Q = uri_string:dissect_query(maps:get(query, U, <<>>)),
    Next = case Path of
        <<"/api/v0/add">> ->
            true = proplists:get_value(<<"nocopy">>, Q) =:= <<"false">>,
            true = proplists:get_value(<<"progress">>, Q) =:= <<"false">>,
            {_, _, Params} = cow_multipart:parse_content_type(proplists:get_value(<<"content-type">>, H)),
            Boundary = proplists:get_value(<<"boundary">>, Params),
            Parts = parts(Body, Boundary, []),
            Parent ! {uploaded_parts, Parts},
            Names = [N || {N,_,_} <- Parts],
            true = length(Names) =:= length(lists:usort(Names)),
            [{RootName, RootType, _} | _] = Parts,
            RootCid = <<"QmYwAPJzv5CZsnAzt8auVZRnGgGmEvFYh7NWU9AXh4rrkJ">>,
            Rows0 = [#{<<"Name">> => N, <<"Hash">> => case N of
                RootName -> RootCid;
                _ -> <<"bafyfixture", (hex(crypto:hash(sha256, N)))/binary>>
            end} || {N,_,_} <- lists:reverse(Parts)],
            Rows = case Mode of
                missing_root -> [#{<<"Name">> => <<"unrelated">>, <<"Hash">> => RootCid}];
                duplicate_root -> Rows0 ++ [#{<<"Name">> => RootName, <<"Hash">> => RootCid}];
                _ -> Rows0
            end,
            Json = iolist_to_binary([[jsx:encode(R), <<"\n">>] || R <- Rows]),
            case Mode of
                add_trailer_error -> trailer_response(Sock, Json, true);
                _ -> response(Sock, 200, Json, <<"application/json; charset=utf-8">>)
            end,
            {RootCid, RootName, RootType, Parts};
        <<"/api/v0/cat">> ->
            {RootCid, RootName, RootType, Parts} = Stored,
            Target = proplists:get_value(<<"arg">>, Q),
            Parent ! {verified_path, Target},
            Rel = case Target of RootCid -> <<>>; _ ->
                Prefix = <<RootCid/binary, "/">>,
                PrefixLen = byte_size(Prefix),
                <<Prefix:PrefixLen/binary, Suffix/binary>> = Target,
                Suffix
            end,
            Name = case {RootType, Rel} of
                {<<"application/x-directory">>, R} -> <<RootName/binary, "/", R/binary>>;
                _ -> RootName
            end,
            {Name, <<"application/octet-stream">>, Data} = lists:keyfind(Name, 1, Parts),
            case Mode of
                corrupt_file -> response(Sock, 200, flip(Data), <<"application/octet-stream">>);
                truncate_file -> response(Sock, 200, truncate(Data), <<"application/octet-stream">>);
                extra_byte -> response(Sock, 200, <<Data/binary, 0>>, <<"application/octet-stream">>);
                cat_error -> response(Sock, 500, <<"do-not-return-body">>, <<"application/json">>);
                cat_trailer_error -> trailer_response(Sock, Data, true);
                cat_trailer_ok -> trailer_response(Sock, Data, false);
                _ -> response(Sock, 200, Data, <<"application/json">>)
            end,
            Stored
    end,
    loop(Sock, B2, Parent, Mode, Next).

parts(Body, Boundary, Acc) ->
    case cow_multipart:parse_headers(Body, Boundary) of
        {ok, H, Rest} ->
            {<<"form-data">>, P} = cow_multipart:parse_content_disposition(proplists:get_value(<<"content-disposition">>, H)),
            Name = uri_string:unquote(proplists:get_value(<<"filename">>, P)),
            Type = proplists:get_value(<<"content-type">>, H),
            {done, Data, Tail} = cow_multipart:parse_body(Rest, Boundary),
            parts(Tail, Boundary, [{Name, Type, Data} | Acc]);
        {done, <<>>} -> lists:reverse(Acc)
    end.

header(Line) ->
    [K,V] = binary:split(Line, <<":">>),
    {string:lowercase(K), string:trim(V)}.

recv(Sock) ->
    case gen_tcp:recv(Sock, 0, 5000) of
        {ok, B} -> B;
        {error, closed} -> exit(normal);
        E -> error(E)
    end.

until(Sock, Buffer, Sep) ->
    case binary:match(Buffer, Sep) of
        {N, Len} ->
            <<Head:N/binary, _:Len/binary, Tail/binary>> = Buffer,
            {Head, Tail};
        nomatch -> until(Sock, <<Buffer/binary, (recv(Sock))/binary>>, Sep)
    end.

take(_Sock, Buffer, N) when byte_size(Buffer) >= N ->
    <<Data:N/binary, Tail/binary>> = Buffer,
    {Data, Tail};
take(Sock, Buffer, N) -> take(Sock, <<Buffer/binary, (recv(Sock))/binary>>, N).

chunks(Sock, Buffer, Acc) ->
    {Line, B1} = until(Sock, Buffer, <<"\r\n">>),
    [Size | _] = binary:split(Line, <<";">>),
    case binary_to_integer(Size, 16) of
        0 ->
            {<<>>, B2} = until(Sock, B1, <<"\r\n">>),
            {iolist_to_binary(lists:reverse(Acc)), B2};
        N ->
            {Data, B2} = take(Sock, B1, N),
            {<<"\r\n">>, B3} = take(Sock, B2, 2),
            chunks(Sock, B3, [Data | Acc])
    end.

response(Sock, Code, Body, Type) ->
    gen_tcp:send(Sock, [<<"HTTP/1.1 ">>, integer_to_binary(Code), <<" Result\r\ncontent-type: ">>, Type,
        <<"\r\ncontent-length: ">>, integer_to_binary(byte_size(Body)), <<"\r\n\r\n">>, Body]).

trailer_response(Sock, Body, Fail) ->
    Trailer = case Fail of true -> <<"x-stream-error: private-error-text\r\n">>; false -> <<>> end,
    Chunk = case Body of <<>> -> []; _ -> [integer_to_binary(byte_size(Body),16), <<"\r\n">>, Body, <<"\r\n">>] end,
    gen_tcp:send(Sock, [<<"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\ntrailer: x-stream-error\r\n\r\n">>,
        Chunk, <<"0\r\n">>, Trailer, <<"\r\n">>]).

flip(<<B,Rest/binary>>) -> <<(B bxor 1),Rest/binary>>;
flip(<<>>) -> <<1>>.
truncate(<<>>) -> <<>>;
truncate(B) -> binary:part(B, 0, byte_size(B)-1).
hex(B) -> string:lowercase(binary:encode_hex(B)).
