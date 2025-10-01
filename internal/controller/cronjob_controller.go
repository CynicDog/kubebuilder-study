/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
// +kubebuilder:docs-gen:collapse=Apache License

package controller

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/robfig/cron"
	kbatch "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ref "k8s.io/client-go/tools/reference"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	batchv1 "tutorial.kubebuilder.io/project/api/v1"
)

// CronJobReconciler reconciles a CronJob object
type CronJobReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Clock
}

type realClock struct{}

type Clock interface {
	Now() time.Time
}

// +kubebuilder:docs-gen:collapse=Clock

func (rc realClock) Now() time.Time { return time.Now() } //nolint:staticcheck

const (
	typeAvailableCronJob   = "Available"
	typeProgressingCronJob = "Progressing"
	typeDegradedCronJob    = "Degraded"

	scheduledTimeAnnotation = "batch.tutorial.kubebuilder.io/scheduled-at"
)

var (
	jobOwnerKey = ".metadata.controller"
)

// +kubebuilder:rbac:groups=batch.tutorial.kubebuilder.io,resources=cronjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch.tutorial.kubebuilder.io,resources=cronjobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=batch.tutorial.kubebuilder.io,resources=cronjobs/finalizers,verbs=update
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs/status,verbs=get

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *CronJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var cronJob batchv1.CronJob
	if err := r.Get(ctx, req.NamespacedName, &cronJob); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("CronJob resource not found, ignoring since object must be deleted")
		}
		log.Error(err, "Unable to fetch CronJob")
		return ctrl.Result{}, err
	}

	if len(cronJob.Status.Conditions) == 0 {
		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type:    typeProgressingCronJob,
			Status:  metav1.ConditionUnknown,
			Reason:  "Reconciling",
			Message: "Start reconciliation",
		})
		if err := r.Status().Update(ctx, &cronJob); err != nil {
			log.Error(err, "Unable to update status")
			return ctrl.Result{}, err
		}

		if err := r.Get(ctx, req.NamespacedName, &cronJob); err != nil {
			log.Error(err, "Unable to fetch CronJob")
			return ctrl.Result{}, err
		}
	}

	var childJobs kbatch.JobList
	if err := r.List(ctx, &childJobs, client.InNamespace(req.Namespace), client.MatchingFields{jobOwnerKey: req.Name}); err != nil {
		log.Error(err, "Unable to list child Jobs")

		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type:    typeDegradedCronJob,
			Status:  metav1.ConditionTrue,
			Reason:  "ReconciliationError",
			Message: fmt.Sprintf("Failed to list childe jobs: %v", err),
		})
		if statusErr := r.Status().Update(ctx, &cronJob); statusErr != nil {
			log.Error(statusErr, "Failed to update Cronjob status")
		}

		return ctrl.Result{}, err
	}

	var activeJobs []*kbatch.Job
	var successfulJobs []*kbatch.Job
	var failedJobs []*kbatch.Job
	var mostRecentTime *time.Time

	// A job is finished when it has a "Complete" or "Failed" conditions marked.
	isJobFinished := func(job *kbatch.Job) (bool, kbatch.JobConditionType) {
		for _, c := range job.Status.Conditions {
			if (c.Type == kbatch.JobComplete || c.Type == kbatch.JobFailed) && c.Status == corev1.ConditionTrue {
				return true, c.Type
			}
		}
		return false, ""
	}
	// +kubebuilder:doc-gen:collapse=isJobFinished

	getScheduleTimeForJob := func(job *kbatch.Job) (*time.Time, error) {
		timeRaw := job.Annotations[scheduledTimeAnnotation]
		if len(timeRaw) == 0 {
			return nil, nil
		}

		timeParsed, err := time.Parse(time.RFC3339, timeRaw)
		if err != nil {
			return nil, err
		}
		return &timeParsed, nil
	}
	// +kubebuilder:docs-gen:collapse=getScheduledTimeForJob

	for i, job := range childJobs.Items {
		_, finishedType := isJobFinished(&job)
		switch finishedType {
		case "":
			activeJobs = append(activeJobs, &childJobs.Items[i])
		case kbatch.JobFailed:
			failedJobs = append(failedJobs, &childJobs.Items[i])
		case kbatch.JobComplete:
			successfulJobs = append(successfulJobs, &childJobs.Items[i])
		}

		scheduledTimeForJob, err := getScheduleTimeForJob(&job)
		if err != nil {
			log.Error(err, "Unable to parse schedule time for child job", "job", &job)
			continue
		}
		if scheduledTimeForJob != nil {
			if mostRecentTime == nil || mostRecentTime.Before(*scheduledTimeForJob) {
				mostRecentTime = scheduledTimeForJob
			}
		}
	}

	if mostRecentTime != nil {
		cronJob.Status.LastScheduleTime = &metav1.Time{Time: *mostRecentTime}
	} else {
		cronJob.Status.LastScheduleTime = nil
	}
	cronJob.Status.Active = nil
	for _, activeJob := range activeJobs {
		jobRef, err := ref.GetReference(r.Scheme, activeJob)
		if err != nil {
			log.Error(err, "Unable to make reference to an active job", "job", activeJob)
			continue
		}
		cronJob.Status.Active = append(cronJob.Status.Active, *jobRef)
	}
	log.V(1).Info("job count", "active jobs", len(activeJobs), "succssful jobs", len(successfulJobs), "failed jobs", len(failedJobs))

	isSuspended := cronJob.Spec.Suspend != nil && *cronJob.Spec.Suspend
	if isSuspended {
		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type:    typeAvailableCronJob,
			Status:  metav1.ConditionFalse,
			Reason:  "Suspended",
			Message: "CronJob is suspended",
		})
	} else if len(failedJobs) > 0 {
		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type:    typeDegradedCronJob,
			Status:  metav1.ConditionTrue,
			Reason:  "JobsFailed",
			Message: fmt.Sprintf("%d job(s) have failed", len(failedJobs)),
		})
		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type:    typeAvailableCronJob,
			Status:  metav1.ConditionFalse,
			Reason:  "JobsFailed",
			Message: fmt.Sprintf("%d job(s) have failed", len(failedJobs)),
		})
	} else if len(activeJobs) > 0 {
		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type:    typeProgressingCronJob,
			Status:  metav1.ConditionTrue,
			Reason:  "JobsActive",
			Message: fmt.Sprintf("%d job(s) are currently active", len(activeJobs)),
		})
		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type:    typeAvailableCronJob,
			Status:  metav1.ConditionTrue,
			Reason:  "JobsActive",
			Message: fmt.Sprintf("Cronjob is progressing with %d active job(s)", len(activeJobs)),
		})
	} else {
		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type:    typeAvailableCronJob,
			Status:  metav1.ConditionTrue,
			Reason:  "AllJobsCompleted",
			Message: "All jobs have completed successfully",
		})
		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type:    typeProgressingCronJob,
			Status:  metav1.ConditionFalse,
			Reason:  "NoJobsActive",
			Message: "No jobs are currenly active",
		})
	}

	// Update the status
	if err := r.Status().Update(ctx, &cronJob); err != nil {
		// We only care about conflict errors and ignore the rest
		if apierrors.IsConflict(err) {
			// The next reconcile will get the latest version and retry all logic.
			log.Info("Failed to update CronJob status due to conflict, will re-reconcile immediately", "error", err)
			return ctrl.Result{}, nil
		}

		// For all other errors, log it and return the error to trigger backoff.
		log.Error(err, "Unable to update CronJob status")
		return ctrl.Result{}, err
	}

	if cronJob.Spec.FailedJobsHistoryLimit != nil {
		sort.Slice(failedJobs, func(i, j int) bool {
			if failedJobs[i].Status.StartTime == nil {
				return failedJobs[j].Status.StartTime != nil
			}
			return failedJobs[i].Status.StartTime.Before(failedJobs[j].Status.StartTime)
		})
		for i, job := range failedJobs {
			if int32(i) >= int32(len(failedJobs))-*cronJob.Spec.FailedJobsHistoryLimit {
				break
			}
			if err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
				log.Error(err, "Unable to delete old failed job", "job", job)
			} else {
				log.V(0).Info("Delete old failed job", "job", job)
			}
		}
	}

	if cronJob.Spec.SuccessfulJobsHistoryLimit != nil {
		sort.Slice(successfulJobs, func(i, j int) bool {
			if successfulJobs[i].Status.StartTime == nil {
				return successfulJobs[j].Status.StartTime != nil
			}
			return successfulJobs[i].Status.StartTime.Before(successfulJobs[j].Status.StartTime)
		})
		for i, job := range successfulJobs {
			if int32(i) >= int32(len(successfulJobs))-*cronJob.Spec.SuccessfulJobsHistoryLimit {
				break
			}
			if err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil {
				log.Error(err, "Unable to delete old successful job", "job", job)
			} else {
				log.V(0).Info("Delete old successful job", "job", job)
			}
		}
	}

	if cronJob.Spec.Suspend != nil && *cronJob.Spec.Suspend {
		log.V(1).Info("cronjob suspended, skipping")
		return ctrl.Result{}, nil
	}

	getNextSchedule := func(cronJob *batchv1.CronJob, now time.Time) (lastMissed time.Time, next time.Time, err error) {
		sched, err := cron.ParseStandard(cronJob.Spec.Schedule)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("unparseable schedule %q: %w", cronJob.Spec.Schedule, err)
		}

		var earliestTime time.Time
		if cronJob.Status.LastScheduleTime != nil {
			earliestTime = cronJob.Status.LastScheduleTime.Time
		} else {
			earliestTime = cronJob.CreationTimestamp.Time
		}
		if cronJob.Spec.StartingDeadlineSeconds != nil {
			schedulingDeadline := now.Add(-time.Second * time.Duration(*cronJob.Spec.StartingDeadlineSeconds))
			if schedulingDeadline.After(earliestTime) {
				earliestTime = schedulingDeadline
			}
		}
		if earliestTime.After(now) {
			return time.Time{}, sched.Next(now), nil
		}

		starts := 0
		for t := sched.Next(earliestTime); !t.After(now); t = sched.Next(t) {
			lastMissed = t

			starts++
			if starts > 100 {
				return time.Time{}, time.Time{}, fmt.Errorf("too many missed start times (> 100). Set or decrease .spec.startingDeadlineSeconds or check clock skew") //nolint:staticcheck
			}
		}
		return lastMissed, sched.Next(now), nil
	}
	// +kubebuilder:doc-gen:collapse=getNextSchedule

	missedRun, nextRun, err := getNextSchedule(&cronJob, r.Now())
	if err != nil {
		log.Error(err, "Unable to figure out CronJob schedule")

		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type:    typeDegradedCronJob,
			Status:  metav1.ConditionTrue,
			Reason:  "InvalideSchedule",
			Message: fmt.Sprintf("Failed to parse schedule: %v", err),
		})
		if statusErr := r.Status().Update(ctx, &cronJob); statusErr != nil {
			log.Error(statusErr, "failed to update CronJob status")
		}
		return ctrl.Result{}, nil
	}

	scheduleResult := ctrl.Result{RequeueAfter: nextRun.Sub(r.Now())}
	log = log.WithValues("now", r.Now(), "next run", nextRun)

	if missedRun.IsZero() {
		log.V(1).Info("No upcoming scheduled times, sleeping until next")
		return scheduleResult, nil
	}

	log = log.WithValues("Current run", missedRun)
	tooLate := false
	if cronJob.Spec.StartingDeadlineSeconds != nil {
		tooLate = missedRun.Add(time.Duration(*cronJob.Spec.StartingDeadlineSeconds) * time.Second).Before(r.Now())
	}
	if tooLate {
		log.V(1).Info("Missed starting deadline for the last run, sleeping til next")

		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type:    typeDegradedCronJob,
			Status:  metav1.ConditionTrue,
			Reason:  "MissedSchedule",
			Message: fmt.Sprintf("Missed starting deadline for run at %v", missedRun),
		})
		if statusErr := r.Status().Update(ctx, &cronJob); statusErr != nil {
			log.Error(statusErr, "Failed to update CronJob status")
		}
		return scheduleResult, nil
	}

	if cronJob.Spec.ConcurrencyPolicy == batchv1.ForbidConcurrent && len(activeJobs) > 0 {
		log.V(1).Info("Concurrency policy blocks concurrent runs, skipping", "num active", len(activeJobs))
		return scheduleResult, nil
	}

	if cronJob.Spec.ConcurrencyPolicy == batchv1.ReplaceConcurrent {
		for _, activeJob := range activeJobs {
			if err := r.Delete(ctx, activeJob, client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
				log.Error(err, "Unable to delete active job", "job", activeJob)
				return ctrl.Result{}, err
			}
		}
	}

	constructJobForCronJob := func(cronJob *batchv1.CronJob, scheduledTime time.Time) (*kbatch.Job, error) {
		name := fmt.Sprintf("%s-%d", cronJob.Name, scheduledTime.Unix())

		job := &kbatch.Job{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      make(map[string]string),
				Annotations: make(map[string]string),
				Name:        name,
				Namespace:   cronJob.Namespace,
			},
			Spec: *cronJob.Spec.JobTemplate.Spec.DeepCopy(),
		}

		for k, v := range cronJob.Spec.JobTemplate.Annotations {
			job.Annotations[k] = v
		}
		job.Annotations[scheduledTimeAnnotation] = scheduledTime.Format(time.RFC3339)

		for k, v := range cronJob.Spec.JobTemplate.Labels {
			job.Labels[k] = v
		}

		if err := ctrl.SetControllerReference(cronJob, job, r.Scheme); err != nil {
			return nil, err
		}

		return job, nil
	}
	// +kubebuilder:docs-gen:collapse=constructJobForCronJob

	job, err := constructJobForCronJob(&cronJob, missedRun)
	if err != nil {
		log.Error(err, "Unable to construct job from template")
		return scheduleResult, nil
	}

	if err := r.Create(ctx, job); err != nil {
		// If the job already exists, we stop trying to create it and continue the reconcile loop.
		// The subsequent status-checking logic will find the existing job.
		if apierrors.IsAlreadyExists(err) {
			log.V(1).Info("Job already exists, likely created by another controller or previous reconcile attempt", "job", job)
		} else {
			log.Error(err, "Unable to create Job for CronJob", "job", job)
			meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
				Type:    typeDegradedCronJob,
				Status:  metav1.ConditionTrue,
				Reason:  "JobCreationFailed",
				Message: fmt.Sprintf("Failed to create job: %v", err),
			})
			if statusErr := r.Status().Update(ctx, &cronJob); statusErr != nil {
				log.Error(statusErr, "Failed to update CronJob status")
			}
			return ctrl.Result{}, err // Return error for actual creation failures
		}
	} else { // Job creation succeeded
		log.V(1).Info("created Job for CronJob run", "job", job)
		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type:    typeProgressingCronJob,
			Status:  metav1.ConditionTrue,
			Reason:  "JobCreated",
			Message: fmt.Sprintf("Created job %s", job.Name),
		})
	}
	if statusErr := r.Status().Update(ctx, &cronJob); statusErr != nil {
		log.Error(statusErr, "Failed to update CronJob status")
	}
	return scheduleResult, nil
}

var (
	apiGVStr = batchv1.GroupVersion.String()
)

// SetupWithManager sets up the controller with the Manager.
func (r *CronJobReconciler) SetupWithManager(mgr ctrl.Manager) error {

	if r.Clock == nil {
		r.Clock = realClock{}
	}

	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &kbatch.Job{}, jobOwnerKey, func(rawObj client.Object) []string {
		job := rawObj.(*kbatch.Job)
		owner := metav1.GetControllerOf(job)

		if owner == nil {
			return nil
		}
		if owner.APIVersion != apiGVStr || owner.Kind != "CronJob" {
			return nil
		}
		return []string{owner.Name}
	}); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&batchv1.CronJob{}).
		Owns(&kbatch.Job{}).
		Named("cronjob").
		Complete(r)
}
