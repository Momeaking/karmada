package aggregatedapiserver

import (
	"github.com/emicklei/go-restful"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"net/http"

	clusterapis "github.com/karmada-io/karmada/pkg/apis/cluster"
	clusterinstall "github.com/karmada-io/karmada/pkg/apis/cluster/install"
	clusterstorage "github.com/karmada-io/karmada/pkg/registry/cluster/storage"
)

var (
	// Scheme defines methods for serializing and deserializing API objects.
	Scheme = runtime.NewScheme()
	// Codecs provides methods for retrieving codecs and serializers for specific
	// versions and content types.
	Codecs = serializer.NewCodecFactory(Scheme)
	// ParameterCodec handles versioning of objects that are converted to query parameters.
	ParameterCodec = runtime.NewParameterCodec(Scheme)
)

func init() {
	clusterinstall.Install(Scheme)

	// we need to add the options to empty v1
	// TODO fix the server code to avoid this
	metav1.AddToGroupVersion(Scheme, schema.GroupVersion{Version: "v1"})

	// TODO: keep the generic API server from wanting this
	unversioned := schema.GroupVersion{Group: "", Version: "v1"}
	Scheme.AddUnversionedTypes(unversioned,
		&metav1.Status{},
		&metav1.APIVersions{},
		&metav1.APIGroupList{},
		&metav1.APIGroup{},
		&metav1.APIResourceList{},
	)
}

// ExtraConfig holds custom apiserver config
type ExtraConfig struct {
	// Add custom config if necessary.
}

// Config defines the config for the apiserver
type Config struct {
	GenericConfig *genericapiserver.RecommendedConfig
	ExtraConfig   ExtraConfig
}

// APIServer contains state for karmada aggregated-apiserver.
type APIServer struct {
	GenericAPIServer *genericapiserver.GenericAPIServer
}

type completedConfig struct {
	GenericConfig genericapiserver.CompletedConfig
	ExtraConfig   *ExtraConfig
}

// CompletedConfig embeds a private pointer that cannot be instantiated outside of this package.
type CompletedConfig struct {
	*completedConfig
}

// Complete fills in any fields not set that are required to have valid data. It's mutating the receiver.
func (cfg *Config) Complete() CompletedConfig {
	c := completedConfig{
		cfg.GenericConfig.Complete(),
		&cfg.ExtraConfig,
	}

	c.GenericConfig.Version = &version.Info{
		Major: "1",
		Minor: "0",
	}

	return CompletedConfig{&c}
}

func (c completedConfig) New(kubeClient kubernetes.Interface) (*APIServer, error) {
	// 构建一个通用的server
	genericServer, err := c.GenericConfig.New("aggregated-apiserver", genericapiserver.NewEmptyDelegate())
	if err != nil {
		return nil, err
	}
	//metricsHandler, err := c.metricsHandler()
	if err != nil {
		return nil, err
	}
	// 构建一个Apiserver
	server := &APIServer{
		GenericAPIServer: genericServer,
	}
	// 构建默认的apiGroup组
	apiGroupInfo := genericapiserver.NewDefaultAPIGroupInfo(clusterapis.GroupName, Scheme, ParameterCodec, Codecs)
	// 构建Storage
	clusterStorage, err := clusterstorage.NewStorage(Scheme, kubeClient, c.GenericConfig.RESTOptionsGetter)
	if err != nil {
		klog.Errorf("unable to create REST storage for a resource due to %v, will die", err)
		return nil, err
	}
	v1alpha1cluster := map[string]rest.Storage{}
	v1alpha1cluster["clusters"] = clusterStorage.Cluster
	v1alpha1cluster["clusters/status"] = clusterStorage.Status
	v1alpha1cluster["clusters/proxy"] = clusterStorage.Proxy
	apiGroupInfo.VersionedResourcesStorageMap["v1alpha1"] = v1alpha1cluster
	// 设置API组
	if err = server.GenericAPIServer.InstallAPIGroup(&apiGroupInfo); err != nil {
		return nil, err
	}
	//server.GenericAPIServer.Handler.NonGoRestfulMux.
	//Install(server.GenericAPIServer.Handler.NonGoRestfulMux)
	//	server.GenericAPIServer.Handler.NonGoRestfulMux.

	return server, nil
}

func (c completedConfig) metricsHandler() (http.HandlerFunc, error) {
	// Return handler that serves metrics from both legacy and Metrics Server registry
	return func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		_, err := w.Write([]byte("hello world"))
		if err != nil {
			return
		}
	}, nil
}

func Install(c *restful.Container) {
	// Set up a service to return the git code version.
	versionWS := new(restful.WebService)
	versionWS.Path("/hello")
	versionWS.Doc("git code version from which this is built")
	versionWS.Route(
		versionWS.GET("/").To(handleVersion).
			Doc("get the code version").
			Operation("getCodeVersion").
			Produces(restful.MIME_JSON).
			Consumes(restful.MIME_JSON))
	c.Add(versionWS)
}
func handleVersion(req *restful.Request, resp *restful.Response) {
	responsewriters.WriteRawJSON(http.StatusOK, "hello word", resp.ResponseWriter)
}
