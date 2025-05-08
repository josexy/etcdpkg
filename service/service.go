package service

import (
	"encoding/json"

	etcdpkg "github.com/josexy/etcdpkg"
)

// ServiceInstance is an instance of a service in a discovery system.
type ServiceInstance struct {
	// ID is the unique instance ID as registered.
	// (Mandatory)
	ID string `json:"id"`
	// Name is the service name as registered.
	// (Mandatory)
	Name string `json:"name"`
	// Version is the version of the compiled.
	// (Optional)
	Version string `json:"version"`
	// Metadata is the kv pair metadata associated with the service instance.
	// (Optional)
	Metadata map[string]string `json:"metadata"`
	// Endpoints is endpoint addresses of the service instance.
	// schema:
	//   http://127.0.0.1:8000?isSecure=false
	//   grpc://127.0.0.1:9000?isSecure=false
	Endpoints []string `json:"endpoints"`
}

func (svc *ServiceInstance) String() string {
	data, _ := json.Marshal(svc)
	return string(data)
}

type serviceInstanceEncoder struct{}
type serviceInstanceDecoder struct{}

func DefaultJSONEncoder() etcdpkg.Encoder[*ServiceInstance] { return &serviceInstanceEncoder{} }

func DefaultJSONDecoder() etcdpkg.Decoder[*ServiceInstance] {
	return &serviceInstanceDecoder{}
}

func (svc *serviceInstanceEncoder) Encode(instance *ServiceInstance) ([]byte, error) {
	return json.Marshal(instance)
}

func (svc *serviceInstanceDecoder) Decode(data []byte) (*ServiceInstance, error) {
	var instance ServiceInstance
	err := json.Unmarshal(data, &instance)
	if err != nil {
		return nil, err
	}
	return &instance, nil
}
