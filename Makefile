all:
	erlc -pz /lib/ejabberd/ebin mod_rdb.erl
	cp mod_rdb.beam /lib/ejabberd/ebin
	ejabberdctl restart
