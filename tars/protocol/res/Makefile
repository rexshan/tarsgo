all:
	tars2go -add-servant=false -tarsPath gitee.com/bee-circle/tarsgo/tars *.tars
	#override file
	sed -i 's|"endpointf"|"gitee.com/bee-circle/tarsgo/tars/protocol/res/endpointf"|g' queryf/QueryF_IF.go
