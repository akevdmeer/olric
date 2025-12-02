package dmap

import (
	"github.com/olric-data/olric/internal/protocol"
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

	latest, err := dm.Function(s.ctx, functionCmd.Key, functionCmd.Function, functionCmd.Arg)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	conn.WriteBulk(latest)
}
