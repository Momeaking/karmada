package runtime

import (
	"context"
	"fmt"
	"reflect"

	clusterv1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"
	policyv1alpha1 "github.com/karmada-io/karmada/pkg/apis/policy/v1alpha1"
	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	"github.com/karmada-io/karmada/pkg/scheduler/framework"
)

// frameworkImpl implements the Framework interface and is responsible for initializing and running scheduler
// plugins.
type frameworkImpl struct {
	scorePluginsWeightMap map[string]int
	filterPlugins         []framework.FilterPlugin
	scorePlugins          []framework.ScorePlugin
}

var _ framework.Framework = &frameworkImpl{}

// NewFramework creates a scheduling framework by registry.
// 创建过滤框架，添加过滤插件、分别是亲和度、容忍度和CRD资源可用情况。
func NewFramework(r Registry) (framework.Framework, error) {
	f := &frameworkImpl{}
	filterPluginsList := reflect.ValueOf(&f.filterPlugins).Elem()
	scorePluginsList := reflect.ValueOf(&f.scorePlugins).Elem()
	filterType := filterPluginsList.Type().Elem()
	scoreType := scorePluginsList.Type().Elem()

	for name, factory := range r {
		p, err := factory()
		if err != nil {
			return nil, fmt.Errorf("failed to initialize plugin %q: %w", name, err)
		}
		// 添加插件到列表中
		addPluginToList(p, filterType, &filterPluginsList)
		addPluginToList(p, scoreType, &scorePluginsList)
	}

	return f, nil
}

// RunFilterPlugins runs the set of configured Filter plugins for resources on the cluster.
// If any of the result is not success, the cluster is not suited for the resource.
//进行集群过滤操作
func (frw *frameworkImpl) RunFilterPlugins(ctx context.Context, placement *policyv1alpha1.Placement, resource *workv1alpha2.ObjectReference, cluster *clusterv1alpha1.Cluster) *framework.Result {
	for _, p := range frw.filterPlugins {
		//判断返回结果是否为Success，如果不是直接退出，如果是接着执行
		if result := p.Filter(ctx, placement, resource, cluster); !result.IsSuccess() {
			return result
		}
	}
	return framework.NewResult(framework.Success)
}

// RunScorePlugins runs the set of configured Filter plugins for resources on the cluster.
// If any of the result is not success, the cluster is not suited for the resource.
func (frw *frameworkImpl) RunScorePlugins(ctx context.Context, placement *policyv1alpha1.Placement, spec *workv1alpha2.ResourceBindingSpec, clusters []*clusterv1alpha1.Cluster) (framework.PluginToClusterScores, error) {
	result := make(framework.PluginToClusterScores, len(frw.filterPlugins))
	// 一级循环，循环的是插件
	for _, p := range frw.scorePlugins {
		var scoreList framework.ClusterScoreList
		// 二级循环查询集群列表
		for _, cluster := range clusters {
			// 分别返回不同过滤器的分数，目前分数都是0
			score, res := p.Score(ctx, placement, spec, cluster)
			if !res.IsSuccess() {
				return nil, fmt.Errorf("plugin %q failed with: %w", p.Name(), res.AsError())
			}
			//scoreList 存放的是不同的集群和对应的分数
			scoreList = append(scoreList, framework.ClusterScore{
				Cluster: cluster,
				Score:   score,
			})
		}

		if p.ScoreExtensions() != nil {
			res := p.ScoreExtensions().NormalizeScore(ctx, scoreList)
			if !res.IsSuccess() {
				return nil, fmt.Errorf("plugin %q normalizeScore failed with: %w", p.Name(), res.AsError())
			}
		}

		weight, ok := frw.scorePluginsWeightMap[p.Name()]
		if !ok {
			result[p.Name()] = scoreList
			continue
		}

		for i := range scoreList {
			scoreList[i].Score = scoreList[i].Score * int64(weight)
		}
		// 存放不同的过滤插件的分数列表
		result[p.Name()] = scoreList
	}

	return result, nil
}

func addPluginToList(plugin framework.Plugin, pluginType reflect.Type, pluginList *reflect.Value) {
	if reflect.TypeOf(plugin).Implements(pluginType) {
		newPlugins := reflect.Append(*pluginList, reflect.ValueOf(plugin))
		pluginList.Set(newPlugins)
	}
}
