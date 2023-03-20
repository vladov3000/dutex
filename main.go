package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/urfave/cli/v2"
)

type Resource struct {
	version    uint64
	expiration time.Time
}

type Dutex struct {
	mutex   sync.Mutex
	locked  map[string]Resource
	version uint64
}

func newDutex() *Dutex {
	return &Dutex{locked: make(map[string]Resource)}
}

type LockArg struct {
	Resource string
	Lifetime time.Duration
}

func (dutex *Dutex) Lock(arg LockArg, version *uint64) error {
	dutex.mutex.Lock()
	defer dutex.mutex.Unlock()

	previous, ok := dutex.locked[arg.Resource]
	if ok && time.Now().Before(previous.expiration) {
		return fmt.Errorf("%s is already locked.", arg.Resource)
	}

	expiration := time.Now().Add(arg.Lifetime)

	dutex.version++
	dutex.locked[arg.Resource] = Resource{dutex.version, expiration}

	*version = dutex.version
	return nil
}

type UnlockArg struct {
	Resource string
	Version  uint64
}

type UnlockReply struct {}

func (dutex *Dutex) Unlock(arg UnlockArg, _ *UnlockReply) error {
	dutex.mutex.Lock()
	defer dutex.mutex.Unlock()

	previous, ok := dutex.locked[arg.Resource]
	if !ok {
		return fmt.Errorf("%s is already unlocked", arg.Resource)
	}
	if arg.Version != previous.version {
		return fmt.Errorf("expected version %d, got version %d",
			uint64(previous.version),
			uint64(arg.Version))
	}

	delete(dutex.locked, arg.Resource)
	return nil
}

func startServer(address string) error {
	dutex := newDutex()
	rpc.Register(dutex)
	rpc.HandleHTTP()
	listener, error := net.Listen("tcp", address)
	if error != nil {
		return error
	}
	return http.Serve(listener, nil)
}

func lock(address string, resource string, lifetime time.Duration) error {
	client, error := rpc.DialHTTP("tcp", address)
	if error != nil {
		return error
	}

	var version uint64
	error = client.Call("Dutex.Lock", LockArg{resource, lifetime}, &version)
	if error != nil {
		return error
	}

	fmt.Printf("Successfully locked %s version %d.\n", resource, uint64(version))
	return nil
}

func unlock(address string, resource string, version uint64) error {
	client, error := rpc.DialHTTP("tcp", address)
	if error != nil {
		return error
	}

	var reply UnlockReply
	error = client.Call("Dutex.Unlock", UnlockArg{resource, version}, &reply)
	if error != nil {
		return error
	}

	fmt.Printf("Successfully unlocked %s version %d.\n", resource, uint64(version))
	return nil
}

func main() {
	app := &cli.App{
		Commands: []*cli.Command{
			{
				Name:  "server",
				Usage: "Run the dutex rpc server",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "address",
						Usage:    "address rpc server should bind to",
						Required: true,
					},
				},
				Action: func(context *cli.Context) error {
					return startServer(context.String("address"))
				},
			},
			{
				Name:  "lock",
				Usage: "Call the lock rpc",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "address",
						Usage:    "address of rpc server",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "resource",
						Usage:    "name of the resource to lock",
						Required: true,
					},
					&cli.StringFlag{
						Name:  "lifetime",
						Value: "1m",
						Usage: "maximum time lock will be held for",
					},
				},
				Action: func(context *cli.Context) error {
					address := context.String("address")
					resource := context.String("resource")
					lifetime, error := time.ParseDuration(context.String("lifetime"))
					if error != nil {
						return error
					}

					return lock(address, resource, lifetime)
				},
			},
			{
				Name:  "unlock",
				Usage: "Call the unlock rpc",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "address",
						Usage:    "address of rpc server",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "resource",
						Usage:    "name of the resource to lock",
						Required: true,
					},
					&cli.Uint64Flag{
						Name:  "version",
						Usage: "the version that was locked",
					},
				},
				Action: func(context *cli.Context) error {
					address := context.String("address")
					resource := context.String("resource")
					version := context.Uint64("version")

					return unlock(address, resource, version)
				},
			},
		},
	}

	if error := app.Run(os.Args); error != nil {
		log.Fatal(error)
	}
}
