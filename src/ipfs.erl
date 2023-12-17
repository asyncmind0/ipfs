-module(ipfs).

-behaviour(gen_server).

-export([start_link/1]).
-export([stop/1]).
-export([ls/2]).
-export([ls/3]).
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

-record(state, {gun :: undefined | pid(), opts :: map()}).

start_link(Opts) -> gen_server:start_link(?MODULE, Opts, []).

stop(Pid) -> gen_server:call(Pid, stop).

ls(Pid, Hash) -> ls(Pid, Hash, 5000).

ls(Pid, Hash, Timeout) ->
  Args = [{<<"arg">>, Hash}],
  gen_server:call(Pid, {get, <<"/ls">>, Args, Timeout}, Timeout).


add(Pid, File) -> add(Pid, File, 5000).

add(Pid, {data, Data, FileName}, Timeout) ->
  gen_server:call(Pid, {add_data, <<"/add">>, [], Data, FileName, Timeout}, Timeout);

add(Pid, {directory, DirectoryPath}, Timeout) ->
  gen_server:call(Pid, {add_directory, <<"/add">>, [], DirectoryPath, Timeout}, Timeout);

add(Pid, File, Timeout) when is_binary(File) -> add(Pid, {file, File}, Timeout);

add(Pid, {file, File}, Timeout) ->
  BaseName = filename:basename(File),
  Args = [{<<"arg">>, BaseName}],
  gen_server:call(Pid, {add_file, <<"/add">>, Args, File, BaseName, Timeout}, Timeout).


cat(Pid, Hash) -> cat(Pid, Hash, 5000).

cat(Pid, Hash, Timeout) ->
  Args = [{<<"arg">>, Hash}],
  gen_server:call(Pid, {get, <<"/cat">>, Args, Timeout}, Timeout).


get(Pid, Hash, FileName) -> get(Pid, Hash, FileName, 5000).

get(Pid, Hash, FileName, Timeout) ->
  Args = [{<<"arg">>, Hash}],
  gen_server:call(Pid, {get_file, <<"/cat">>, Args, FileName, Timeout}, Timeout).


init(Opts) -> {ok, #state{opts = Opts}, {continue, start_gun}}.

handle_call({get, URI, Args, Timeout}, _From, State) ->
  StreamRef = gun:get(State#state.gun, format_uri(URI, Args)),
  Response = wait_response(State#state.gun, StreamRef, Timeout),
  {reply, Response, State};

handle_call({get_file, URI, Args, FileName, Timeout}, _From, State) ->
  case file:open(FileName, [write, raw, binary]) of
    {ok, FD} ->
      StreamRef = gun:get(State#state.gun, format_uri(URI, Args)),
      wait_response(State#state.gun, StreamRef, fun (Data) -> file:write(FD, Data) end, Timeout),
      file:close(FD),
      {reply, ok, State};

    Error -> {reply, Error, State}
  end;

handle_call({add_file, URI, Args, File, BaseName, Timeout}, _From, State) ->
  case file:open(File, [read, binary, raw]) of
    {ok, FD} ->
      Boundary = cow_multipart:boundary(),
      StreamRef =
        gun:post(
          State#state.gun,
          format_uri(URI, Args),
          [{<<"content-type">>, <<"multipart/form-data; boundary=", Boundary/binary>>}]
        ),
      Data =
        iolist_to_binary(
          [
            cow_multipart:part(
              Boundary,
              [
                {
                  <<"content-disposition">>,
                  <<"form-data; name=\"filename\"; filename=\"", BaseName/binary, "\"">>
                },
                {<<"content-type">>, <<"application/octet-stream">>}
              ]
            )
          ]
        ),
      gun:data(State#state.gun, StreamRef, nofin, Data),
      do_sendfile(State#state.gun, StreamRef, Boundary, FD),
      Response = wait_response(State#state.gun, StreamRef, Timeout),
      {reply, Response, State};

    Error -> {reply, Error, State}
  end;

handle_call({add_data, URI, Args, Data, FileName, Timeout}, _From, State) ->
  Boundary = cow_multipart:boundary(),
  StreamRef =
    gun:post(
      State#state.gun,
      format_uri(URI, Args),
      [{<<"content-type">>, <<"multipart/form-data; boundary=", Boundary/binary>>}]
    ),
  InitData =
    iolist_to_binary(
      [
        cow_multipart:part(
          Boundary,
          [
            {
              <<"content-disposition">>,
              <<"form-data; name=\"filename\"; filename=\"", FileName/binary, "\"">>
            },
            {<<"content-type">>, <<"application/octet-stream">>}
          ]
        )
      ]
    ),
  gun:data(State#state.gun, StreamRef, nofin, InitData),
  gun:data(State#state.gun, StreamRef, nofin, Data),
  gun:data(State#state.gun, StreamRef, fin, cow_multipart:close(Boundary)),
  Response = wait_response(State#state.gun, StreamRef, Timeout),
  {reply, Response, State};

handle_call({add_directory, URI, Args, DirectoryPath, Timeout}, _From, State) ->
  Boundary = cow_multipart:boundary(),
  %{ok, DirectoryContents} = file:list_dir(DirectoryPath),
  StreamRef =
    gun:post(
      State#state.gun,
      format_uri(URI, Args),
      [{<<"content-type">>, <<"multipart/form-data; boundary=", Boundary/binary>>}]
    ),
  ok = stream_multipart_body(State#state.gun, StreamRef, Boundary, DirectoryPath),
  gun:data(State#state.gun, StreamRef, fin, cow_multipart:close(Boundary)),
  Response = wait_response(State#state.gun, StreamRef, Timeout),
  {reply, Response, State};

handle_call(stop, _From, State) -> {stop, normal, ok, State}.


handle_cast(_Req, State) -> {noreply, State}.

handle_info({gun_response, _Pid, Resp, nofin, Status, _Headers}, State) ->
  logger:info("response , reason: ~p status ~p", [Resp, Status]),
  {noreply, State};

handle_info({gun_data, _Pid, Resp, fin, <<>>}, State) ->
  logger:info("response , data fin: ~p ", [Resp]),
  {noreply, State};

handle_info({gun_data, _Pid, _Resp, nofin, Data}, State) ->
  logger:info("response , data: ~p ", [Data]),
  {noreply, State};

handle_info({gun_up, _Pid, _Proto}, State) -> {noreply, State};

handle_info({gun_down, _Pid, _Proto, Reason, _KilledStreams}, State) ->
  logger:error("connection down, reason: ~p", [Reason]),
  {noreply, State};

handle_info({'DOWN', _Ref, process, _Pid, Reason}, State) ->
  logger:error("connection terminated, reason: ~p", [Reason]),
  {noreply, State, {continue, start_gun}}.


handle_continue(start_gun, #state{opts = #{ip := IP} = Opts} = State) ->
  GunOpts =
    #{
      retry => application:get_env(?MODULE, http_retry, 5),
      retry_timeout => application:get_env(?MODULE, http_retry_timeout, 5000),
      http_opts => #{keepalive => infinity}
    },
  {ok, Gun} = gun:open(IP, maps:get(port, Opts, 5001), GunOpts),
  erlang:monitor(process, Gun),
  {noreply, State#state{gun = Gun}}.


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

    %{data, fin, _Data} when Acc =:= <<>> ->
    %    {ok, InitStatus, <<>>};
    {data, fin, <<>>} when CT =:= <<"application/json">> ->
      {ok, InitStatus, Acc};

    {data, fin, Data} when CT =:= <<"application/json">> ->
      {ok, InitStatus, <<Acc/binary, Data/binary>>};

    {data, fin, Data} ->
      {ok, InitStatus, <<Acc/binary, Data/binary>>};

    Error -> Error
  end.


do_sendfile(Pid, StreamRef, Boundary, FD) ->
  case file:read(FD, 1048576) of
    {ok, Data} ->
      gun:data(Pid, StreamRef, nofin, Data),
      do_sendfile(Pid, StreamRef, Boundary, FD);

    eof ->
      gun:data(Pid, StreamRef, fin, cow_multipart:close(Boundary)),
      file:close(FD)
  end.


do_sendfile_nofin(Pid, StreamRef, Boundary, FD) ->
  case file:read(FD, 1048576) of
    {ok, Data} ->
      gun:data(Pid, StreamRef, nofin, Data),
      do_sendfile_nofin(Pid, StreamRef, Boundary, FD);

    eof ->
      gun:data(Pid, StreamRef, nofin, <<"\n">>),
      file:close(FD)
  end.


recursively_add_dir(DirectoryPath, GunState, StreamRef, Boundary) ->
  {ok, Files} = file:list_dir(DirectoryPath),
  directory_part(GunState, StreamRef, Boundary, DirectoryPath),
  lists:foreach(
    fun
      (File) ->
        FilePath = filename:join(DirectoryPath, File),
        case filelib:is_dir(FilePath) of
          true ->
            directory_part(GunState, StreamRef, Boundary, File),
            recursively_add_dir(FilePath, GunState, StreamRef, Boundary);

          false ->
            file_part(GunState, StreamRef, Boundary, DirectoryPath, File),
            ok
        end
    end,
    Files
  ).


stream_multipart_body(GunState, StreamRef, Boundary, DirectoryPath) ->
  recursively_add_dir(DirectoryPath, GunState, StreamRef, Boundary),
  ok.


% Function to create a directory part in the multipart/form-data request
directory_part(GunState, StreamRef, Boundary, DirectoryName) ->
  DirectoryNameBin = list_to_binary(DirectoryName),
  InitData =
    iolist_to_binary(
      [
        cow_multipart:part(
          Boundary,
          [
            {
              <<"Content-Disposition">>,
              <<"form-data; name=\"file\"; filename=\"", DirectoryNameBin/binary, "\"">>
            },
            {<<"Content-Type">>, <<"application/x-directory">>}
          ]
        )
      ]
    ),
  gun:data(GunState, StreamRef, nofin, InitData).


% Function to create a file part in the multipart/form-data request
file_part(GunState, StreamRef, Boundary, DirectoryPath, FileName) ->
  FileNameBin = list_to_binary(FileName),
  DirectoryPathBin = list_to_binary(DirectoryPath),
  InitData =
    iolist_to_binary(
      [
        cow_multipart:part(
          Boundary,
          [
            {
              <<"Content-Disposition">>,
              <<
                "form-data; name=\"",
                FileNameBin/binary,
                "\"; filename=\"",
                DirectoryPathBin/binary,
                "/",
                FileNameBin/binary,
                "\""
              >>
            },
            {<<"Content-Type">>, <<"application/octet-stream">>},
            {<<"Abspath">>, <<"/", DirectoryPathBin/binary, "/", FileNameBin/binary>>}
          ]
        )
      ]
    ),
  gun:data(GunState, StreamRef, nofin, InitData),
  case file:open(filename:join(DirectoryPath, FileName), [read, binary, raw]) of
    {ok, FD} -> do_sendfile_nofin(GunState, StreamRef, Boundary, FD);
    Error -> logger:error("error File part ~p ", [Error])
  end.
