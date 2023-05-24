package dmap

import (
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/tidwall/redcon"
)

func (s *Service) functionCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	functionCmd, err := protocol.ParseFunctionCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	dm, err := s.getOrCreateDMap(functionCmd.DMap)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	e := newEnv(s.ctx)
	e.dmap = dm.name
	e.key = functionCmd.Key
	e.function = functionCmd.Function
	e.value = functionCmd.Arg
	latest, err := dm.function(e)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	conn.WriteBulk(latest)
}
