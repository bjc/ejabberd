%%%----------------------------------------------------------------------
%%% File    : mod_version.erl
%%% Author  : Alexey Shchepin <alexey@sevcom.net>
%%% Purpose : 
%%% Created : 18 Jan 2003 by Alexey Shchepin <alexey@sevcom.net>
%%% Id      : $Id$
%%%----------------------------------------------------------------------

-module(mod_version).
-author('alexey@sevcom.net').
-vsn('$Revision$ ').

-behaviour(gen_mod).

-export([start/1,
	 stop/0,
	 process_local_iq/3]).

-include("ejabberd.hrl").
-include("namespaces.hrl").



start(Opts) ->
    IQDisc = gen_mod:get_opt(iqdisc, Opts, one_queue),
    gen_iq_handler:add_iq_handler(ejabberd_local, ?NS_VERSION,
				  ?MODULE, process_local_iq, IQDisc).

stop() ->
    gen_iq_handler:remove_iq_handler(ejabberd_local, ?NS_VERSION).


process_local_iq(From, To, {iq, ID, Type, XMLNS, SubEl}) ->
    case Type of
	set ->
	    {iq, ID, error, XMLNS,
	     [SubEl, {xmlelement, "error",
		      [{"code", "405"}],
		      [{xmlcdata, "Not Allowed"}]}]};
	get ->
	    OSType = case os:type() of
			 {Osfamily, Osname} ->
			     atom_to_list(Osfamily) ++ "/" ++
				 atom_to_list(Osname);
			 Osfamily ->
			     atom_to_list(Osfamily)
		     end,
	    OSVersion = case os:version() of
			    {Major, Minor, Release} ->
				lists:flatten(
				  io_lib:format("~w.~w.~w",
						[Major, Minor, Release]));
			    VersionString ->
				VersionString
			end,
	    OS = OSType ++ " " ++ OSVersion,
	    {iq, ID, result, XMLNS,
	     [{xmlelement, "query",
	       [{"xmlns", ?NS_VERSION}],
	       [{xmlelement, "name", [],
		 [{xmlcdata, "ejabberd"}]},
		{xmlelement, "version", [],
		 [{xmlcdata, ?VERSION}]},
		{xmlelement, "os", [],
		 [{xmlcdata, OS}]}
	       ]}]}
    end.

