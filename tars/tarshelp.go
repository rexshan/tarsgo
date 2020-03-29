package tars

import "sync"

type ICommunicator interface {
	GetServantProxy(objname string) *ServantProxy
}


var(
	pbonce          sync.Once
	gPbComm 		*Communicator
	once            sync.Once
	gFrameworkComm  *Communicator
)


func NewPbCommunicator() *Communicator {
	pbonce.Do(func() {
		startFrameWorkComm()
		c := new(Communicator)
		if GetClientConfig() != nil {
			c.Client = GetClientConfig()
			c.SetProperty("netconnectionnum",c.Client.netconnectionnum)
		} else {
			c.Client = &clientConfig{
				Locator:"",
				stat:"",
				property:"",
				modulename:"",
				refreshEndpointInterval:60000,
				reportInterval:10000,
			}
			c.SetProperty("netconnectionnum",2)
		}
		if GetServerConfig() != nil {
			c.SetProperty("notify", GetServerConfig().notify)
			c.SetProperty("node", GetServerConfig().Node)
			c.SetProperty("server", GetServerConfig().Server)
		}
		c.s = NewServantProxyFactory(c)
		gPbComm = c
	})

	return gPbComm
}

func startFrameWorkComm() *Communicator {
	once.Do(func() {
		c := new(Communicator)
		//c.init()
		if GetClientConfig() != nil {
			c.Client = GetClientConfig()
			c.SetProperty("netconnectionnum", c.Client.netconnectionnum)
		} else {
			c.Client = &clientConfig{
				Locator:"",
				stat:"",
				property:"",
				modulename:"",
				refreshEndpointInterval:60000,
				reportInterval:10000,
			}
			c.SetProperty("netconnectionnum", 2)
		}

		if GetServerConfig() != nil {
			c.SetProperty("netthread", GetServerConfig().netThread)
			c.SetProperty("notify", GetServerConfig().notify)
			c.SetProperty("node", GetServerConfig().Node)
			c.SetProperty("server", GetServerConfig().Server)
			c.SetProperty("isclient", false)
		}

		c.s = NewServantProxyFactory(c)
		if GetClientConfig() != nil {
			c.setQueryPrx(GetClientConfig().Locator)
			c.SetLocator(GetClientConfig().Locator)
		}
		c.SetProperty("netconnectionnum", 1)
		gFrameworkComm = c
	})
	return gFrameworkComm
}