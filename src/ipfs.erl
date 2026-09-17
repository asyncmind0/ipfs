-module(ipfs).

-behaviour(gen_server).

-export([start_link/1]).
-export([stop/1]).
-export([version/1]).
-export([ls/2]).
-export([ls/3]).
-export([pin/2]).
-export([add/2]).
-export([add/3]).
-export([cat/2]).
-export([cat/3]).
-export([get/3]).
-export([get/4]).
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([handle_continue/2]).
-export([terminate/2]).
-include_lib("kernel/include/logger.hrl").
-include_lib("kernel/include/file.hrl").

-record(state, {gun :: undefined | pid(), opts :: map()}).

-define(DEFAULT_TIMEOUT, 5000).
-define(UPLOAD_CHUNK_BYTES, 1048576).
-define(MAX_ADD_RESPONSE_BYTES, 16777216).

%% Pure serialization/response helpers only; never export_all in tests.
-ifdef(TEST).
-export([upload_name/1, part_headers/2, add_result/1, root_cid/2,
         verify_digest/3, check_stream_error/1]).
-endif.

start_link(Opts) -> gen_server:start_link(?MODULE, Opts, []).

stop(Pid) -> gen_server:call(Pid, stop).

version(Pid) -> gen_server:call(Pid, {version, [], ?DEFAULT_TIMEOUT}, ?DEFAULT_TIMEOUT).

ls(Pid, Hash) -> ls(Pid, Hash, ?DEFAULT_TIMEOUT).

ls(Pid, Hash, Timeout) ->
  Args = [{<<"arg">>, Hash}],
  gen_server:call(Pid, {ls, Args, Timeout}, Timeout).

pin(Pid, Hashes) ->
  gen_server:call(Pid, {pin, <<"/pin/add">>, Hashes}, ?DEFAULT_TIMEOUT).
    
add(Pid, File) -> add(Pid, File, ?DEFAULT_TIMEOUT).

add(Pid, {data, Data, FileName}, Timeout) ->
  gen_server:call(Pid, {add_data, <<"/add">>, [], Data, FileName, Timeout}, Timeout);

add(Pid, {directory, DirectoryPath}, Timeout) ->
  gen_server:call(Pid, {add_directory, <<"/add">>, [], DirectoryPath, Timeout}, Timeout);

add(Pid, File, Timeout) when is_binary(File) -> add(Pid, {file, File}, Timeout);

add(Pid, {file, File}, Timeout) ->
  BaseName = filename:basename(File),
  Args = [{<<"arg">>, BaseName}],
  gen_server:call(Pid, {add_file, <<"/add">>, Args, File, BaseName, Timeout}, Timeout).


cat(Pid, Hash) -> cat(Pid, Hash, ?DEFAULT_TIMEOUT).

cat(Pid, Hash, Timeout) ->
  Args = [{<<"arg">>, Hash}],
  gen_server:call(Pid, {cat, Args, Timeout}, Timeout).


get(Pid, Hash, FileName) -> get(Pid, Hash, FileName, ?DEFAULT_TIMEOUT).

get(Pid, Hash, FileName, Timeout) ->
  Args = [{<<"arg">>, Hash}],
  gen_server:call(Pid, {get_file, <<"/cat">>, Args, FileName, Timeout}, Timeout).


init(Opts) -> {ok, #state{opts = Opts}, {continue, start_gun}}.

handle_call({get, URI, Args, Timeout}, _From, State) ->
  ?LOG_INFO("ipfs request ~p", [format_uri(URI, Args)]),
  StreamRef = gun:get(State#state.gun, format_uri(URI, Args)),
  Response = wait_response(State#state.gun, StreamRef, Timeout),
  {reply, Response, State};

handle_call({get_file, URI, Args, FileName, Timeout}, _From, State) ->
  case file:open(FileName, [write, raw, binary]) of
    {ok, FD} ->
      StreamRef =
        gun:post(State#state.gun, format_uri(URI, Args), [{<<"content-type">>, <<"text/plain">>}]),
      wait_response(State#state.gun, StreamRef, fun (Data) -> file:write(FD, Data) end, Timeout),
      file:close(FD),
      {reply, ok, State};

    Error -> {reply, Error, State}
  end;

handle_call({pin, URI, Hashes,  Timeout}, _From, State) ->
      StreamRef =
        gun:post(State#state.gun, format_uri(URI, {<<"arg">> ,Hashes}), [{<<"content-type">>, <<"text/plain">>}]),
      Response = wait_response(State#state.gun, StreamRef, Timeout),
      {reply, Response, State};
handle_call({add_file, _URI, _Args, File, BaseName, Timeout}, _From, State) ->
  Response = upload(State#state.gun, {file, File, BaseName}, Timeout),
  {reply, Response, State};

handle_call({cat, Args, Timeout}, _From, State) ->
  URI = <<"/cat">>,
  StreamRef =
    gun:post(State#state.gun, format_uri(URI, Args), [{<<"content-type">>, <<"text/plain">>}]),
  Response = wait_response(State#state.gun, StreamRef, Timeout),
  {reply, Response, State};

handle_call({ls, Args, Timeout}, _From, State) ->
  URI = <<"/ls">>,
  StreamRef =
    gun:post(State#state.gun, format_uri(URI, Args), [{<<"content-type">>, <<"text/plain">>}]),
  Response = wait_response(State#state.gun, StreamRef, Timeout),
  {reply, Response, State};

handle_call({add_data, _URI, _Args, Data, FileName, Timeout}, _From, State) ->
  Response = upload(State#state.gun, {data, Data, FileName}, Timeout),
  {reply, Response, State};

handle_call({add_directory, _URI, _Args, DirectoryPath, Timeout}, _From, State) ->
  Response = upload(State#state.gun, {directory, DirectoryPath}, Timeout),
  {reply, Response, State};

handle_call({version, Args, Timeout}, _From, State) ->
  URI = <<"/version">>,
  StreamRef =
    gun:post(State#state.gun, format_uri(URI, Args), [{<<"content-type">>, <<"text/plain">>}]),
  Response = wait_response(State#state.gun, StreamRef, Timeout),
  {reply, Response, State};

handle_call(stop, _From, State) -> {stop, normal, ok, State}.


handle_cast(_Req, State) -> {noreply, State}.

handle_info({gun_response, _Pid, Resp, nofin, Status, _Headers}, State) ->
  ?LOG_INFO("response , reason: ~p status ~p", [Resp, Status]),
  {noreply, State};

handle_info({gun_data, _Pid, Resp, fin, <<>>}, State) ->
  ?LOG_INFO("response , data fin: ~p ", [Resp]),
  {noreply, State};

handle_info({gun_data, _Pid, _Resp, nofin, Data}, State) ->
  ?LOG_INFO("response , data bytes: ~p", [byte_size(Data)]),
  {noreply, State};

%% Cancelled uploads can still have already-delivered terminal messages.
handle_info({gun_data, _Pid, _Ref, fin, _Data}, State) -> {noreply, State};
handle_info({gun_response, _Pid, _Ref, fin, _Status, _Headers}, State) -> {noreply, State};
handle_info({gun_trailers, _Pid, _Ref, _Headers}, State) -> {noreply, State};
handle_info({gun_error, _Pid, _Ref, _Reason}, State) -> {noreply, State};
handle_info({gun_error, _Pid, _Reason}, State) -> {noreply, State};

handle_info({gun_up, _Pid, _Proto}, State) -> {noreply, State};

handle_info({gun_down, _Pid, _Proto, normal, _KilledStreams}, State) ->
  {noreply, State};
handle_info({gun_down, _Pid, _Proto, Reason, _KilledStreams}, State) ->
  ?LOG_ERROR("connection down, reason: ~p, state: ~p", [Reason, State]),
  {noreply, State};

handle_info({'DOWN', _Ref, process, _Pid, {shutdown, econnrefused}}, State) ->
  ?LOG_DEBUG(
  "connection refused, state: ~p", [State]),
  {noreply, State, {continue, start_gun}};
handle_info({'DOWN', _Ref, process, _Pid, Reason}, State) ->
  ?LOG_ERROR(
  "connection down, reason: ~p, state: ~p", [Reason, State]),
  {noreply, State, {continue, start_gun}}.


handle_continue(start_gun, #state{opts = Opts} = State) ->
    IP = maps:get(ip, Opts),
    Port = maps:get(port, Opts, 5001),

    GunOpts = #{
        retry => application:get_env(?MODULE, http_retry, 5),
        retry_timeout => application:get_env(?MODULE, http_retry_timeout, ?DEFAULT_TIMEOUT),
        http_opts => #{keepalive => infinity}
    },

    case gun:open(IP, Port, GunOpts) of
        {ok, Gun} ->
            erlang:monitor(process, Gun),
            {noreply, State#state{gun = Gun}};
        {error, Reason} ->
            ?LOG_ERROR("gun open failed ip=~p port=~p reason=~p", [IP, Port, Reason]),
            {stop, Reason, State}
    end.


terminate(_Reason, State) -> gun:close(State#state.gun).

format_uri(URI, QS) -> <<"/api/v0", URI/binary, "?", (cow_qs:qs(QS))/binary>>.

wait_response(Pid, StreamRef, Timeout) -> wait_response(Pid, StreamRef, <<>>, Timeout).

wait_response(Pid, StreamRef, Acc, Timeout) ->
  case wait_response(Pid, StreamRef, undefined, undefined, Acc, Timeout) of
    {ok, 200, Data} -> {ok, Data};
    {ok, _Status, Data} -> {error, Data};
    Error -> Error
  end.


wait_response(Pid, StreamRef, InitStatus, CT, Acc, Timeout) ->
  case gun:await(Pid, StreamRef, Timeout) of
    {response, nofin, Status, Headers} ->
      NewCT = proplists:get_value(<<"content-type">>, Headers, CT),
      wait_response(Pid, StreamRef, Status, NewCT, Acc, Timeout);

    {response, fin, Status, _Headers} -> {ok, Status, Acc};

    {data, nofin, Data} when is_function(Acc) ->
      Acc(Data),
      wait_response(Pid, StreamRef, InitStatus, CT, Acc, Timeout);

    {data, nofin, Data} ->
      wait_response(Pid, StreamRef, InitStatus, CT, <<Acc/binary, Data/binary>>, Timeout);

    {data, fin, Data} when is_function(Acc) ->
      Acc(Data),
      {ok, InitStatus, Acc};

    {data, fin, <<>>} when CT =:= <<"application/json">> ->
      {ok, InitStatus, [jsx:decode(A) || A <- string:split(Acc, "\n", all), A =/= <<>>]};

    {data, fin, Data} when CT =:= <<"application/json">> ->
      {
        ok,
        InitStatus,
        [jsx:decode(A) || A <- string:split(<<Acc/binary, Data/binary>>, "\n", all), A =/= <<>>]
      };

    {data, fin, Data} -> {ok, InitStatus, <<Acc/binary, Data/binary>>};
    Error -> Error
  end.


%% ------------------------------------------------------------------
%% Verified uploads. The multipart payload is always raw file bytes.
%% Cowlib's part/2 and close/1 ALREADY supply the delimiter CRLF. Never
%% append a newline to a file body (including an empty file or a symlink).
%%
%% Keep historical add response Names: they may include source ancestry.
%% Verify regular files through the exact uploaded root, not through an
%% arbitrary last response entry or a standalone child CID.
%% ------------------------------------------------------------------
upload(Pid, What, Timeout) ->
  try
    Deadline = upload_deadline(Timeout),
    RootName = upload_root(What),
    ok = preflight_upload(What, Deadline),
    Boundary = cow_multipart:boundary(),
    Args = [{<<"progress">>, <<"false">>},
            {<<"wrap-with-directory">>, <<"false">>},
            {<<"nocopy">>, <<"false">>}],
    StreamRef = gun:headers(Pid, <<"POST">>, format_uri(<<"/add">>, Args),
      [{<<"content-type">>, <<"multipart/form-data; boundary=", Boundary/binary>>},
       {<<"accept-encoding">>, <<"identity">>}, {<<"te">>, <<"trailers">>}], #{flow => 1}),
    try
      Inventory = send_upload(Pid, StreamRef, Boundary, What, Deadline),
      ok = gun:data(Pid, StreamRef, fin, cow_multipart:close(Boundary)),
      Body = upload_response(Pid, StreamRef, Deadline,
        fun(Data, Acc) -> [Data | Acc] end, [], ?MAX_ADD_RESPONSE_BYTES),
      Rows = add_result(iolist_to_binary(lists:reverse(Body))),
      RootCid = root_cid(Rows, RootName),
      ok = verify_upload(Pid, RootCid, Inventory, Deadline),
      _ = upload_remaining(Deadline),
      {ok, Rows}
    after
      cancel_upload(Pid, StreamRef)
    end
  catch
    throw:{upload_error, Reason} -> {error, Reason};
    Class:Reason ->
      %% Do not leak payloads or callback arguments in a caught exception.
      {error, {upload_exception, Class, exception_tag(Reason)}}
  end.

upload_root({directory, Path}) -> upload_name(Path);
upload_root({file, _Path, Name}) -> upload_name(Name);
upload_root({data, _Data, Name}) -> upload_name(Name).

preflight_upload({directory, Path}, Deadline) ->
  _ = upload_remaining(Deadline),
  case file:read_link_info(Path) of
    {ok, #file_info{type = directory}} -> preflight_directory(Path, Deadline);
    {ok, #file_info{type = Type}} -> upload_fail({not_a_directory, Path, Type});
    {error, Why} -> upload_fail({file_info_failed, Path, Why})
  end;
preflight_upload({file, Path, _Name}, Deadline) ->
  preflight_file(Path, Deadline);
preflight_upload({data, Data, _Name}, Deadline) ->
  _ = upload_remaining(Deadline),
  _ = iolist_size(Data),
  ok.

preflight_directory(Path, Deadline) ->
  _ = upload_name(Path),
  lists:foreach(fun(Name) ->
    _ = upload_remaining(Deadline),
    Child = filename:join(Path, Name),
    _ = upload_name(Child),
    case file:read_link_info(Child) of
      {ok, #file_info{type = directory}} -> preflight_directory(Child, Deadline);
      {ok, #file_info{type = regular}} -> preflight_file(Child, Deadline);
      {ok, #file_info{type = symlink}} -> _ = link_target(Child), ok;
      {ok, #file_info{type = Type}} -> upload_fail({unsupported_file_type, Child, Type});
      {error, Why} -> upload_fail({file_info_failed, Child, Why})
    end
  end, directory_entries(Path)),
  ok.

preflight_file(Path, Deadline) ->
  _ = upload_remaining(Deadline),
  case file:read_link_info(Path) of
    {ok, #file_info{type = regular}} ->
      case file:open(Path, [read, binary, raw]) of
        {ok, FD} -> ok = file:close(FD);
        {error, Why} -> upload_fail({file_open_failed, Path, Why})
      end;
    {ok, #file_info{type = Type}} -> upload_fail({unsupported_file_type, Path, Type});
    {error, Why} -> upload_fail({file_info_failed, Path, Why})
  end.

send_upload(Pid, Ref, Boundary, {directory, Path}, Deadline) ->
  send_directory(Pid, Ref, Boundary, Path, <<>>, Deadline, []);
send_upload(Pid, Ref, Boundary, {file, Path, Name}, Deadline) ->
  [send_file(Pid, Ref, Boundary, Path, upload_name(Name), <<>>, Deadline)];
send_upload(Pid, Ref, Boundary, {data, Data, Name}, Deadline) ->
  _ = upload_remaining(Deadline),
  ok = send_part(Pid, Ref, Boundary, upload_name(Name), <<"application/octet-stream">>),
  ok = gun:data(Pid, Ref, nofin, Data),
  [#{path => <<>>, bytes => iolist_size(Data), sha256 => crypto:hash(sha256, Data)}].

send_directory(Pid, Ref, Boundary, Path, Relative, Deadline, Acc) ->
  _ = upload_remaining(Deadline),
  %% Emit each directory once, including empty directories.
  ok = send_part(Pid, Ref, Boundary, upload_name(Path), <<"application/x-directory">>),
  lists:foldl(fun(Name, A) ->
    Child = filename:join(Path, Name),
    Rel = join_upload_path(Relative, name_binary(Name)),
    _ = upload_remaining(Deadline),
    case file:read_link_info(Child) of
      {ok, #file_info{type = directory}} ->
        send_directory(Pid, Ref, Boundary, Child, Rel, Deadline, A);
      {ok, #file_info{type = regular}} ->
        [send_file(Pid, Ref, Boundary, Child, upload_name(Child), Rel, Deadline) | A];
      {ok, #file_info{type = symlink}} ->
        Target = link_target(Child),
        ok = send_part(Pid, Ref, Boundary, upload_name(Child), <<"application/symlink">>),
        ok = gun:data(Pid, Ref, nofin, Target),
        A;
      {ok, #file_info{type = Type}} -> upload_fail({unsupported_file_type, Child, Type});
      {error, Why} -> upload_fail({file_info_failed, Child, Why})
    end
  end, Acc, directory_entries(Path)).

send_file(Pid, Ref, Boundary, Path, Name, Relative, Deadline) ->
  %% Preflight is not a substitute for checking again at the point of use.
  ok = preflight_file(Path, Deadline),
  case file:open(Path, [read, raw, binary]) of
    {ok, FD} ->
      try
        {ok, Before} = file:read_file_info(FD),
        ok = send_part(Pid, Ref, Boundary, Name, <<"application/octet-stream">>),
        {Digest, Bytes} = send_file_bytes(Pid, Ref, FD, Deadline, crypto:hash_init(sha256), 0),
        {ok, After} = file:read_file_info(FD),
        case Bytes =:= Before#file_info.size andalso
             Before#file_info.size =:= After#file_info.size andalso
             Before#file_info.mtime =:= After#file_info.mtime andalso
             Before#file_info.ctime =:= After#file_info.ctime of
          true -> #{path => Relative, bytes => Bytes, sha256 => Digest};
          false -> upload_fail({upload_source_changed, Name})
        end
      after
        file:close(FD)
      end;
    {error, Why} -> upload_fail({file_open_failed, Path, Why})
  end.

send_file_bytes(Pid, Ref, FD, Deadline, Hash, Bytes) ->
  _ = upload_remaining(Deadline),
  case file:read(FD, ?UPLOAD_CHUNK_BYTES) of
    {ok, Data} ->
      ok = gun:data(Pid, Ref, nofin, Data),
      send_file_bytes(Pid, Ref, FD, Deadline,
        crypto:hash_update(Hash, Data), Bytes + byte_size(Data));
    eof -> {crypto:hash_final(Hash), Bytes};
    {error, Why} -> upload_fail({file_read_failed, Why})
  end.

send_part(Pid, Ref, Boundary, Name, Type) ->
  gun:data(Pid, Ref, nofin, cow_multipart:part(Boundary, part_headers(Name, Type))).

part_headers(Name, Type) ->
  %% Kubo URL-decodes multipart filenames. Encode literal percent, quotes,
  %% backslashes and non-ASCII bytes so their file identities are preserved.
  Encoded = uri_string:quote(Name),
  [{<<"content-disposition">>,
    <<"form-data; name=\"file\"; filename=\"", Encoded/binary, "\"">>},
   {<<"content-type">>, Type}].

%% Keep the caller-visible path convention, but reject ambiguous components.
%% Leading/trailing separators are normalized exactly as Kubo add names are.
upload_name(Path0) ->
  Path = name_binary(Path0),
  Parts = [P || P <- binary:split(Path, <<"/">>, [global]), P =/= <<>>, P =/= <<".">>],
  case Parts =/= [] andalso not lists:member(<<"..">>, Parts) andalso
       binary:match(Path, [<<0>>, <<"\r">>, <<"\n">>]) =:= nomatch of
    true -> iolist_to_binary(lists:join(<<"/">>, Parts));
    false -> upload_fail(invalid_upload_name)
  end.

name_binary(B) when is_binary(B) -> B;
name_binary(L) when is_list(L) ->
  case unicode:characters_to_binary(L) of
    B when is_binary(B) -> B;
    _ -> upload_fail(invalid_upload_name)
  end.

join_upload_path(<<>>, Name) -> Name;
join_upload_path(Parent, Name) -> <<Parent/binary, "/", Name/binary>>.

directory_entries(Path) ->
  case file:list_dir(Path) of
    {ok, Files} -> lists:sort(Files);
    {error, Why} -> upload_fail({list_dir_failed, Path, Why})
  end.

link_target(Path) ->
  case file:read_link(Path) of
    {ok, Target} -> name_binary(Target);
    {error, Why} -> upload_fail({read_link_failed, Path, Why})
  end.

%% Do not trust HTTP 200 or a returned CID alone. Read every regular file
%% through the selected root; this also detects misbound directory entries.
verify_upload(Pid, Root, Inventory, Deadline) ->
  lists:foreach(fun(#{path := Rel, bytes := Bytes, sha256 := Expected}) ->
    Target = join_upload_path(Root, Rel),
    Ref = gun:post(Pid, format_uri(<<"/cat">>, [{<<"arg">>, Target}]),
      [{<<"accept-encoding">>, <<"identity">>}, {<<"te">>, <<"trailers">>}],
      <<>>, #{flow => 1}),
    try
      {Hash, ActualBytes} = upload_response(Pid, Ref, Deadline,
        fun(Data, {H, N}) -> {crypto:hash_update(H, Data), N + byte_size(Data)} end,
        {crypto:hash_init(sha256), 0}, Bytes),
      ok = verify_digest(Target, {Expected, Bytes}, {crypto:hash_final(Hash), ActualBytes})
    after
      cancel_upload(Pid, Ref)
    end
  end, lists:reverse(Inventory)),
  ok.

verify_digest(Target, {Expected, Bytes}, {Actual, ActualBytes}) ->
  case {Actual =:= Expected, ActualBytes =:= Bytes} of
    {true, true} -> ok;
    _ -> upload_fail({upload_integrity_mismatch, #{
      path => Target, expected_bytes => Bytes, actual_bytes => ActualBytes,
      expected_sha256 => hex(Expected), actual_sha256 => hex(Actual)}})
  end.

hex(Bin) -> string:lowercase(binary:encode_hex(Bin)).

root_cid(Rows, Name) ->
  Matches = [Hash || #{<<"Name">> := N, <<"Hash">> := Hash} <- Rows,
    root_name_equal(N, Name), is_binary(Hash), byte_size(Hash) > 0],
  case Matches of
    [Hash] -> Hash;
    [] -> upload_fail({add_root_missing, Name});
    _ -> upload_fail({add_root_ambiguous, Name})
  end.

root_name_equal(Name, Wanted) ->
  %% Some importers include an unnamed wrapper. It is not the requested root.
  try upload_name(Name) =:= Wanted
  catch throw:{upload_error, _} -> false end.

add_result(Body) ->
  Lines = [string:trim(L) || L <- binary:split(Body, <<"\n">>, [global]),
    string:trim(L) =/= <<>>],
  Rows = lists:map(fun(L) ->
    Row = try jsx:decode(L, [return_maps]) catch _:_ -> upload_fail(invalid_add_response) end,
    case Row of
      #{<<"Name">> := Name, <<"Hash">> := Hash} when is_binary(Name), is_binary(Hash), Hash =/= <<>> -> Row;
      _ -> upload_fail(invalid_add_response)
    end
  end, Lines),
  case Rows of [] -> upload_fail(empty_add_response); _ -> Rows end.

%% Separate raw reader for upload acknowledgements and verification; never
%% JSON-decode package bytes based on the response Content-Type. Bound both
%% response sizes and the deadline, and reject trailer-reported errors.
upload_response(Pid, Ref, Deadline, Fold, Acc, MaxBytes) ->
  case gun:await(Pid, Ref, upload_remaining(Deadline)) of
    {response, Fin, 200, Headers} ->
      ok = check_stream_error(Headers),
      case Fin of
        fin -> Acc;
        nofin -> upload_response_body(Pid, Ref, Deadline, Fold, Acc, MaxBytes, 0)
      end;
    {inform, _Status, _Headers} -> upload_response(Pid, Ref, Deadline, Fold, Acc, MaxBytes);
    {response, _Fin, Status, _Headers} -> upload_fail({upload_http_status, Status});
    {error, Why} -> upload_fail({upload_transport_failed, transport_tag(Why)});
    _ -> upload_fail(invalid_upload_response)
  end.

upload_response_body(Pid, Ref, Deadline, Fold, Acc, Limit, Seen) ->
  case gun:await(Pid, Ref, upload_remaining(Deadline)) of
    {data, Fin, Data} ->
      Count = Seen + byte_size(Data),
      case Count =< Limit of
        false -> upload_fail({upload_response_too_large, Limit});
        true ->
          Next = Fold(Data, Acc),
          case Fin of
            fin -> _ = upload_remaining(Deadline), Next;
            nofin ->
              gun:update_flow(Pid, Ref, 1),
              upload_response_body(Pid, Ref, Deadline, Fold, Next, Limit, Count)
          end
      end;
    {trailers, Headers} ->
      ok = check_stream_error(Headers),
      _ = upload_remaining(Deadline),
      Acc;
    {error, Why} -> upload_fail({upload_transport_failed, transport_tag(Why)});
    _ -> upload_fail(invalid_upload_response)
  end.

check_stream_error(Headers) ->
  case lists:any(fun({K, V}) -> string:lowercase(K) =:= <<"x-stream-error">> andalso V =/= <<>> end, Headers) of
    true -> upload_fail(upload_stream_error);
    false -> ok
  end.

upload_deadline(infinity) -> infinity;
upload_deadline(Timeout) when is_integer(Timeout), Timeout > 0 ->
  erlang:monotonic_time(millisecond) + Timeout;
upload_deadline(_) -> upload_fail(invalid_upload_timeout).

upload_remaining(infinity) -> infinity;
upload_remaining(Deadline) ->
  case Deadline - erlang:monotonic_time(millisecond) of
    N when N > 0 -> N;
    _ -> upload_fail(upload_timeout)
  end.

cancel_upload(Pid, Ref) ->
  try gun:cancel(Pid, Ref) catch _:_ -> ok end.

transport_tag({stream_error, Why}) -> transport_tag(Why);
transport_tag({connection_error, Why}) -> transport_tag(Why);
transport_tag(Why) when is_atom(Why) -> Why;
transport_tag(_) -> connection_failed.

exception_tag(Reason) when is_atom(Reason) -> Reason;
exception_tag(_) -> failed.

upload_fail(Reason) -> throw({upload_error, Reason}).
