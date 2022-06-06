package core

import (
	"context"
	"fmt"
	"time"

	"k8s.io/klog/v2"

	clusterv1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"
	policyv1alpha1 "github.com/karmada-io/karmada/pkg/apis/policy/v1alpha1"
	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	"github.com/karmada-io/karmada/pkg/scheduler/cache"
	"github.com/karmada-io/karmada/pkg/scheduler/core/spreadconstraint"
	"github.com/karmada-io/karmada/pkg/scheduler/framework"
	"github.com/karmada-io/karmada/pkg/scheduler/framework/runtime"
	"github.com/karmada-io/karmada/pkg/scheduler/metrics"
)

// ScheduleAlgorithm is the interface that should be implemented to schedule a resource to the target clusters.
type ScheduleAlgorithm interface {
	Schedule(context.Context, *policyv1alpha1.Placement, *workv1alpha2.ResourceBindingSpec) (scheduleResult ScheduleResult, err error)
}

// ScheduleResult includes the clusters selected.
type ScheduleResult struct {
	SuggestedClusters []workv1alpha2.TargetCluster
}

type genericScheduler struct {
	schedulerCache    cache.Cache
	scheduleFramework framework.Framework
}

// NewGenericScheduler creates a genericScheduler object.
// 构建通用调度器
func NewGenericScheduler(
	schedCache cache.Cache,
	registry runtime.Registry,
) (ScheduleAlgorithm, error) {
	f, err := runtime.NewFramework(registry)
	if err != nil {
		return nil, err
	}
	return &genericScheduler{
		schedulerCache:    schedCache,
		scheduleFramework: f,
	}, nil
}

func (g *genericScheduler) Schedule(ctx context.Context, placement *policyv1alpha1.Placement, spec *workv1alpha2.ResourceBindingSpec) (result ScheduleResult, err error) {
	// 1、获取集群快照信息，
	clusterInfoSnapshot := g.schedulerCache.Snapshot()
	// 2、检测数量是否为0
	if clusterInfoSnapshot.NumOfClusters() == 0 {
		return result, fmt.Errorf("no clusters available to schedule")
	}
	// 3、得到可以部署工作负载的集群列表
	feasibleClusters, err := g.findClustersThatFit(ctx, g.scheduleFramework, placement, &spec.Resource, clusterInfoSnapshot)
	if err != nil {
		return result, fmt.Errorf("failed to findClustersThatFit: %v", err)
	}
	if len(feasibleClusters) == 0 {
		return result, fmt.Errorf("no clusters fit")
	}
	klog.V(4).Infof("feasible clusters found: %v", feasibleClusters)
	// 4、集群分值计算，感觉全部都是0
	clustersScore, err := g.prioritizeClusters(ctx, g.scheduleFramework, placement, spec, feasibleClusters)
	if err != nil {
		return result, fmt.Errorf("failed to prioritizeClusters: %v", err)
	}
	klog.V(4).Infof("feasible clusters scores: %v", clustersScore)
	// 5、根据拓扑关系约束进行集群选择，如果有返回传播约束的集群，没有返回 feasibleClusters，分数并没有用到
	// 根据工作负载需要部署的集群数量进行选择，如果可用集群的数量小于需要部署的集群数量，直接返回作为可用集群，如果大于，则选取前面的几个进行部署
	clusters, err := g.selectClusters(clustersScore, placement, spec)
	if err != nil {
		return result, fmt.Errorf("failed to select clusters: %v", err)
	}
	klog.V(4).Infof("selected clusters: %v", clusters)
	// 6、副本数，分配副本数 返回集群名称、副本数量 ，副本数是可选的
	clustersWithReplicas, err := g.assignReplicas(clusters, placement.ReplicaScheduling, spec)
	if err != nil {
		return result, fmt.Errorf("failed to assignReplicas: %v", err)
	}
	result.SuggestedClusters = clustersWithReplicas

	return result, nil
}

// findClustersThatFit finds the clusters that are fit for the placement based on running the filter plugins.
func (g *genericScheduler) findClustersThatFit(
	ctx context.Context,
	fwk framework.Framework,
	placement *policyv1alpha1.Placement,
	resource *workv1alpha2.ObjectReference,
	clusterInfo *cache.Snapshot) ([]*clusterv1alpha1.Cluster, error) {
	defer metrics.ScheduleStep(metrics.ScheduleStepFilter, time.Now())

	var out []*clusterv1alpha1.Cluster
	// 获取状态为Ready的集群
	clusters := clusterInfo.GetReadyClusters()
	// 循环所有的集群，查找可用的集群。集群数量过多的话，可能会导致遍历时间过长。这里也没有用到k8s中 16个 go run去验证。
	for _, c := range clusters {
		if result := fwk.RunFilterPlugins(ctx, placement, resource, c.Cluster()); !result.IsSuccess() {
			klog.V(4).Infof("cluster %q is not fit, reason: %v", c.Cluster().Name, result.AsError())
		} else {
			out = append(out, c.Cluster())
		}
	}

	return out, nil
}

// prioritizeClusters prioritize the clusters by running the score plugins.
func (g *genericScheduler) prioritizeClusters(
	ctx context.Context,
	fwk framework.Framework,
	placement *policyv1alpha1.Placement,
	spec *workv1alpha2.ResourceBindingSpec,
	clusters []*clusterv1alpha1.Cluster) (result framework.ClusterScoreList, err error) {
	defer metrics.ScheduleStep(metrics.ScheduleStepScore, time.Now())
	//核心方法，计算分数
	scoresMap, err := fwk.RunScorePlugins(ctx, placement, spec, clusters)
	if err != nil {
		return result, err
	}

	if klog.V(4).Enabled() {
		for plugin, nodeScoreList := range scoresMap {
			klog.Infof("Plugin %s scores on %v/%v => %v", plugin, spec.Resource.Namespace, spec.Resource.Name, nodeScoreList)
		}
	}
	// 分数的计算方式
	result = make(framework.ClusterScoreList, len(clusters))
	for i := range clusters {
		result[i] = framework.ClusterScore{Cluster: clusters[i], Score: 0}
		// 3个循环，计算每个集群经过三个过滤插件之后的分数之和
		for j := range scoresMap {
			result[i].Score += scoresMap[j][i].Score
		}
	}

	return result, nil
}

func (g *genericScheduler) selectClusters(clustersScore framework.ClusterScoreList,
	placement *policyv1alpha1.Placement, spec *workv1alpha2.ResourceBindingSpec) ([]*clusterv1alpha1.Cluster, error) {
	defer metrics.ScheduleStep(metrics.ScheduleStepSelect, time.Now())

	groupClustersInfo := spreadconstraint.GroupClustersWithScore(clustersScore, placement, spec, calAvailableReplicas)
	// 根据集群分组信息选择集群
	return spreadconstraint.SelectBestClusters(placement, groupClustersInfo, spec.Replicas)
}

func (g *genericScheduler) assignReplicas(
	clusters []*clusterv1alpha1.Cluster,
	replicaSchedulingStrategy *policyv1alpha1.ReplicaSchedulingStrategy,
	object *workv1alpha2.ResourceBindingSpec,
) ([]workv1alpha2.TargetCluster, error) {
	defer metrics.ScheduleStep(metrics.ScheduleStepAssignReplicas, time.Now())
	if len(clusters) == 0 {
		return nil, fmt.Errorf("no clusters available to schedule")
	}
	targetClusters := make([]workv1alpha2.TargetCluster, len(clusters))

	if object.Replicas > 0 && replicaSchedulingStrategy != nil {
		switch replicaSchedulingStrategy.ReplicaSchedulingType {
		// 1. Duplicated Scheduling
		case policyv1alpha1.ReplicaSchedulingTypeDuplicated:
			for i, cluster := range clusters {
				targetClusters[i] = workv1alpha2.TargetCluster{Name: cluster.Name, Replicas: object.Replicas}
			}
			return targetClusters, nil
		// 2. Divided Scheduling
		case policyv1alpha1.ReplicaSchedulingTypeDivided:
			switch replicaSchedulingStrategy.ReplicaDivisionPreference {
			// 2.1 Weighted Scheduling
			case policyv1alpha1.ReplicaDivisionPreferenceWeighted:
				// If ReplicaDivisionPreference is set to "Weighted" and WeightPreference is not set,
				// scheduler will weight all clusters averagely.
				if replicaSchedulingStrategy.WeightPreference == nil {
					replicaSchedulingStrategy.WeightPreference = getDefaultWeightPreference(clusters)
				}
				// 2.1.1 Dynamic Weighted Scheduling (by resource)
				if len(replicaSchedulingStrategy.WeightPreference.DynamicWeight) != 0 {
					return divideReplicasByDynamicWeight(clusters, replicaSchedulingStrategy.WeightPreference.DynamicWeight, object)
				}
				// 2.1.2 Static Weighted Scheduling
				return divideReplicasByStaticWeight(clusters, replicaSchedulingStrategy.WeightPreference.StaticWeightList, object.Replicas)
			// 2.2 Aggregated scheduling (by resource)
			case policyv1alpha1.ReplicaDivisionPreferenceAggregated:
				return divideReplicasByResource(clusters, object, policyv1alpha1.ReplicaDivisionPreferenceAggregated)
			default:
				return nil, fmt.Errorf("undefined replica division preference: %s", replicaSchedulingStrategy.ReplicaDivisionPreference)
			}
		default:
			return nil, fmt.Errorf("undefined replica scheduling type: %s", replicaSchedulingStrategy.ReplicaSchedulingType)
		}
	}

	for i, cluster := range clusters {
		targetClusters[i] = workv1alpha2.TargetCluster{Name: cluster.Name}
	}
	return targetClusters, nil
}
