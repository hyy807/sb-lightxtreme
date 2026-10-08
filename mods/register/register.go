package register

import (
	"github.com/sagernet/sing-box/adapter/outbound"
	C "github.com/sagernet/sing-box/constant"
	MC "github.com/sagernet/sing-box/mods/modconst"
	"github.com/sagernet/sing-box/mods/protocol/lightxtreme"
)

func init()                                         { C.ModProxyDisplayName = MC.DisplayName }
func RegisterOutbounds(registry *outbound.Registry) { lightxtreme.RegisterOutbound(registry) }
