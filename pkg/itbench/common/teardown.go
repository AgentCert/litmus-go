package common

import (
	"context"
	"fmt"
	"strings"

	"github.com/litmuschaos/litmus-go/pkg/log"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

// Helm 3 stamps this label + these annotations onto every object it applies, and
// records release state as Secrets of this type named "sh.helm.release.v1.<rel>.vN".
// A dependency-free `helm uninstall` is: delete everything carrying that metadata for
// the release, plus the state Secrets. The ACE agent/app charts define no Helm hooks,
// so there is nothing hook-ordered to preserve.
const (
	helmManagedByLabel   = "app.kubernetes.io/managed-by=Helm"
	helmReleaseNameAnno  = "meta.helm.sh/release-name"
	helmReleaseNsAnno    = "meta.helm.sh/release-namespace"
	helmReleaseSecretTyp = "helm.sh/release.v1"
)

// helmSweptNamespacedGVRs is the set of namespaced resource kinds an ACE agent/app
// Helm chart can create. Missing APIs on a given cluster are tolerated (logged, skipped).
var helmSweptNamespacedGVRs = []schema.GroupVersionResource{
	{Group: "apps", Version: "v1", Resource: "deployments"},
	{Group: "apps", Version: "v1", Resource: "statefulsets"},
	{Group: "apps", Version: "v1", Resource: "daemonsets"},
	{Group: "apps", Version: "v1", Resource: "replicasets"},
	{Group: "", Version: "v1", Resource: "services"},
	{Group: "", Version: "v1", Resource: "configmaps"},
	{Group: "", Version: "v1", Resource: "secrets"},
	{Group: "", Version: "v1", Resource: "serviceaccounts"},
	{Group: "", Version: "v1", Resource: "persistentvolumeclaims"},
	{Group: "batch", Version: "v1", Resource: "jobs"},
	{Group: "batch", Version: "v1", Resource: "cronjobs"},
	{Group: "autoscaling", Version: "v2", Resource: "horizontalpodautoscalers"},
	{Group: "networking.k8s.io", Version: "v1", Resource: "ingresses"},
	{Group: "networking.k8s.io", Version: "v1", Resource: "networkpolicies"},
	{Group: "policy", Version: "v1", Resource: "poddisruptionbudgets"},
	{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"},
	{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"},
}

// helmSweptClusterGVRs is the set of cluster-scoped kinds an ACE chart can create.
var helmSweptClusterGVRs = []schema.GroupVersionResource{
	{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"},
	{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"},
}

// UninstallHelmRelease removes every object Helm 3 recorded as belonging to release
// `releaseName` deployed into `namespace` -- identified by the
// app.kubernetes.io/managed-by=Helm label plus the meta.helm.sh/release-name annotation
// Helm stamps on all managed objects -- and then Helm's own release-state Secrets. It is
// a dependency-free stand-in for `helm uninstall <releaseName> -n <namespace>
// --ignore-not-found` (the itbench-experiment image is distroless and ships no helm CLI).
//
// Individual object deletes are best-effort (logged, not fatal) so a half-applied or
// partially-hand-deleted release still gets cleaned up; only a wholesale inability to
// list resources is returned as an error.
func UninstallHelmRelease(ctx context.Context, kube kubernetes.Interface, dyn dynamic.Interface, releaseName, namespace string) error {
	if releaseName == "" || namespace == "" {
		return fmt.Errorf("uninstall: releaseName and namespace are both required (got release=%q ns=%q)", releaseName, namespace)
	}
	log.Infof("[uninstall] Removing Helm release %q from namespace %q", releaseName, namespace)

	background := metav1.DeletePropagationBackground
	delOpts := metav1.DeleteOptions{PropagationPolicy: &background}
	listOpts := metav1.ListOptions{LabelSelector: helmManagedByLabel}

	deleted, listFailures := 0, 0

	sweep := func(gvr schema.GroupVersionResource, namespaced bool) {
		if namespaced {
			l, err := dyn.Resource(gvr).Namespace(namespace).List(ctx, listOpts)
			if err != nil {
				if isMissingAPI(err) {
					return
				}
				log.Warnf("[uninstall] list %s in %s failed: %v", gvr.Resource, namespace, err)
				listFailures++
				return
			}
			for _, obj := range l.Items {
				annos := obj.GetAnnotations()
				if annos[helmReleaseNameAnno] != releaseName {
					continue
				}
				if err := dyn.Resource(gvr).Namespace(namespace).Delete(ctx, obj.GetName(), delOpts); err != nil && !apierrors.IsNotFound(err) {
					log.Warnf("[uninstall] delete %s/%s failed: %v", gvr.Resource, obj.GetName(), err)
					continue
				}
				log.Infof("[uninstall] deleted %s/%s", gvr.Resource, obj.GetName())
				deleted++
			}
			return
		}

		l, err := dyn.Resource(gvr).List(ctx, listOpts)
		if err != nil {
			if isMissingAPI(err) {
				return
			}
			log.Warnf("[uninstall] list %s (cluster) failed: %v", gvr.Resource, err)
			listFailures++
			return
		}
		for _, obj := range l.Items {
			annos := obj.GetAnnotations()
			if annos[helmReleaseNameAnno] != releaseName || annos[helmReleaseNsAnno] != namespace {
				continue
			}
			if err := dyn.Resource(gvr).Delete(ctx, obj.GetName(), delOpts); err != nil && !apierrors.IsNotFound(err) {
				log.Warnf("[uninstall] delete %s/%s failed: %v", gvr.Resource, obj.GetName(), err)
				continue
			}
			log.Infof("[uninstall] deleted %s/%s (cluster-scoped)", gvr.Resource, obj.GetName())
			deleted++
		}
	}

	for _, gvr := range helmSweptNamespacedGVRs {
		sweep(gvr, true)
	}
	for _, gvr := range helmSweptClusterGVRs {
		sweep(gvr, false)
	}

	// Helm's own release-state Secrets.
	if secrets, err := kube.CoreV1().Secrets(namespace).List(ctx, metav1.ListOptions{}); err == nil {
		prefix := "sh.helm.release.v1." + releaseName + "."
		for _, s := range secrets.Items {
			if string(s.Type) != helmReleaseSecretTyp || !strings.HasPrefix(s.Name, prefix) {
				continue
			}
			if err := kube.CoreV1().Secrets(namespace).Delete(ctx, s.Name, delOpts); err != nil && !apierrors.IsNotFound(err) {
				log.Warnf("[uninstall] delete release secret %s failed: %v", s.Name, err)
				continue
			}
			log.Infof("[uninstall] deleted release secret %s", s.Name)
			deleted++
		}
	} else {
		log.Warnf("[uninstall] list secrets in %s failed: %v", namespace, err)
		listFailures++
	}

	log.Infof("[uninstall] release %q: %d object(s) deleted, %d list failure(s)", releaseName, deleted, listFailures)
	if deleted == 0 && listFailures > 0 {
		return fmt.Errorf("uninstall: could not enumerate any resources for release %q in %s (%d list failures)", releaseName, namespace, listFailures)
	}
	return nil
}

// DeleteNamespace deletes namespace ns and returns nil if it is already gone. This is
// the cascade-everything catch-all: it removes every namespaced object regardless of
// how (or whether) it was created -- fault leftovers, orphaned PVCs, the app's Helm
// release, ChaosEngine/ChaosResult CRs -- so teardown is complete even for a run that
// errored partway through. `--wait=false` semantics: the call returns as soon as the
// namespace is marked for deletion; kube-controller-manager finalizes the cascade.
func DeleteNamespace(ctx context.Context, kube kubernetes.Interface, ns string) error {
	if ns == "" {
		return fmt.Errorf("delete-namespace: namespace is required")
	}
	log.Infof("[uninstall] Deleting namespace %q (cascades to all contained objects)", ns)
	background := metav1.DeletePropagationBackground
	err := kube.CoreV1().Namespaces().Delete(ctx, ns, metav1.DeleteOptions{PropagationPolicy: &background})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete namespace %q: %w", ns, err)
	}
	if apierrors.IsNotFound(err) {
		log.Infof("[uninstall] namespace %q already absent", ns)
	}
	return nil
}

// isMissingAPI is true when a List failed because the cluster does not serve that
// group/version/resource at all (e.g. autoscaling/v2 on an older cluster) -- as opposed
// to a real transient/permission error, which the caller should surface.
func isMissingAPI(err error) bool {
	if err == nil {
		return false
	}
	if apierrors.IsNotFound(err) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "could not find the requested resource") ||
		strings.Contains(msg, "no matches for kind") ||
		strings.Contains(msg, "the server doesn't have a resource type")
}
