package datadoggenericresource

import (
	"context"
	"encoding/json"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/DataDog/datadog-operator/api/datadoghq/v1alpha1"
)

type LogsMetricsHandler struct{}

func (h *LogsMetricsHandler) createResourcefunc(r *Reconciler, logger logr.Logger, instance *v1alpha1.DatadogGenericResource, status *v1alpha1.DatadogGenericResourceStatus, now metav1.Time, hash string) error {
	createdMetric, err := createLogsMetric(r.datadogAuth, r.datadogLogsMetricsClient, instance)
	if err != nil {
		logger.Error(err, "error creating logs metric")
		updateErrStatus(status, now, v1alpha1.DatadogSyncStatusCreateError, "CreatingCustomResource", err)
		return err
	}
	logger.Info("created a new logs metric", "metric Id", createdMetric.Data.GetId())
	status.Id = createdMetric.Data.GetId()
	status.Created = &now
	status.LastForceSyncTime = &now
	status.Creator = ""
	status.SyncStatus = v1alpha1.DatadogSyncStatusOK
	status.CurrentHash = hash
	return nil
}

func (h *LogsMetricsHandler) getResourcefunc(r *Reconciler, instance *v1alpha1.DatadogGenericResource) error {
	_, err := getLogsMetric(r.datadogAuth, r.datadogLogsMetricsClient, instance.Status.Id)
	return err
}

func (h *LogsMetricsHandler) updateResourcefunc(r *Reconciler, instance *v1alpha1.DatadogGenericResource) error {
	_, err := updateLogsMetric(r.datadogAuth, r.datadogLogsMetricsClient, instance)
	return err
}

func (h *LogsMetricsHandler) deleteResourcefunc(r *Reconciler, instance *v1alpha1.DatadogGenericResource) error {
	return deleteLogsMetric(r.datadogAuth, r.datadogLogsMetricsClient, instance.Status.Id)
}

func getLogsMetric(auth context.Context, client *datadogV2.LogsMetricsApi, metricID string) (datadogV2.LogsMetricResponse, error) {
	metric, _, err := client.GetLogsMetric(auth, metricID)
	if err != nil {
		return datadogV2.LogsMetricResponse{}, translateClientError(err, "error getting logs metric")
	}
	return metric, nil
}

func deleteLogsMetric(auth context.Context, client *datadogV2.LogsMetricsApi, metricID string) error {
	if _, err := client.DeleteLogsMetric(auth, metricID); err != nil {
		return translateClientError(err, "error deleting logs metric")
	}
	return nil
}

func createLogsMetric(auth context.Context, client *datadogV2.LogsMetricsApi, instance *v1alpha1.DatadogGenericResource) (datadogV2.LogsMetricResponse, error) {
	metricBody := &datadogV2.LogsMetricCreateRequest{}
	json.Unmarshal([]byte(instance.Spec.JsonSpec), metricBody)
	metric, _, err := client.CreateLogsMetric(auth, *metricBody)
	if err != nil {
		return datadogV2.LogsMetricResponse{}, translateClientError(err, "error creating logs metric")
	}
	return metric, nil
}

func updateLogsMetric(auth context.Context, client *datadogV2.LogsMetricsApi, instance *v1alpha1.DatadogGenericResource) (datadogV2.LogsMetricResponse, error) {
	metricUpdateData := &datadogV2.LogsMetricUpdateRequest{}
	json.Unmarshal([]byte(instance.Spec.JsonSpec), metricUpdateData)
	metricUpdated, _, err := client.UpdateLogsMetric(auth, instance.Status.Id, *metricUpdateData)
	if err != nil {
		return datadogV2.LogsMetricResponse{}, translateClientError(err, "error updating logs metric")
	}
	return metricUpdated, nil
}