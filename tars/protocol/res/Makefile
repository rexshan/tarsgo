all:
	tars2go -add-servant=false -tarsPath github.com/rexshan/tarsgo/tars *.tars
	#override file
	sed -i 's|"endpointf"|"github.com/rexshan/tarsgo/tars/protocol/res/endpointf"|g' queryf/QueryF_IF.go
