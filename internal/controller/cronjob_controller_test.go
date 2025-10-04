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

package controller

import (
	"context"
	"reflect"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/apimachinery/pkg/types"

	cronjobv1 "tutorial.kubebuilder.io/project/api/v1"
)

// +kubebuilder:docs-gen:collapse=Imports

func hasCondition(conditions []metav1.Condition, conditionType string, expectedStatus metav1.ConditionStatus) bool {
	for _, condition := range conditions {
		if condition.Type == conditionType && condition.Status == expectedStatus {
			return true
		}
	}
	return false
}

var _ = Describe("CronJob Controller", func() {

	const (
		CronJobName      = "test-cronjob"
		CronJobNamespace = "default"
		JobName          = "test-job"

		timeout  = time.Second * 10
		duration = time.Second * 10
		interval = time.Millisecond * 250
	)

	Context("When updating CronJob Status", func() {
		It("Should increase CronJob Status.Active count when new Jobs are created", func() {
			By("Creating a new CronJob")
			ctx := context.Background()
			cronJob := &cronjobv1.CronJob{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "batch.tutorial.kubebuilder.io/v1",
					Kind:       "CronJob",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      CronJobName,
					Namespace: CronJobNamespace,
				},
				Spec: cronjobv1.CronJobSpec{
					Schedule: "1 * * * *",
					JobTemplate: batchv1.JobTemplateSpec{
						Spec: batchv1.JobSpec{
							Template: v1.PodTemplateSpec{
								Spec: v1.PodSpec{
									Containers: []v1.Container{
										{
											Name:  "test-container",
											Image: "test-image",
										},
									},
									RestartPolicy: v1.RestartPolicyOnFailure,
								},
							},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, cronJob)).To(Succeed())

			cronjobLookupKey := types.NamespacedName{
				Name:      CronJobName,
				Namespace: CronJobNamespace,
			}
			createdCronjob := &cronjobv1.CronJob{}

			Eventually(func(g Gomega) {
				g.Expect(k8sClient.Get(ctx, cronjobLookupKey, createdCronjob)).To(Succeed())
			}, timeout, interval).Should(Succeed())

			Expect(createdCronjob.Spec.Schedule).To(Equal("1 * * * *"))

			// Test that CronJob.Status.Active correctly tracks active downstream Jobs, starting with none.
			By("By checking the CronJob has zero active Jobs")
			Consistently(func(g Gomega) {
				g.Expect(k8sClient.Get(ctx, cronjobLookupKey, createdCronjob)).To(Succeed())
				g.Expect(createdCronjob.Status.Active).To(BeEmpty())
			}, duration, interval).Should(Succeed())

			// Create a stub Job with active pods and set its owner to the test CronJob.
			By("By creating a new job ")
			testJob := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      JobName,
					Namespace: CronJobNamespace,
				},
				Spec: batchv1.JobSpec{
					Template: v1.PodTemplateSpec{
						Spec: v1.PodSpec{
							Containers: []v1.Container{
								{
									Name:  "test-container",
									Image: "test-image",
								},
							},
							RestartPolicy: v1.RestartPolicyOnFailure,
						},
					},
				},
			}

			kind := reflect.TypeOf(cronjobv1.CronJob{}).Name()
			gvk := cronjobv1.GroupVersion.WithKind(kind)

			controllerRef := metav1.NewControllerRef(createdCronjob, gvk)
			testJob.SetOwnerReferences([]metav1.OwnerReference{*controllerRef})

			Expect(k8sClient.Create(ctx, testJob)).To(Succeed())

			testJob.Status.Active = 1
			Expect(k8sClient.Status().Update(ctx, testJob)).To(Succeed())

			By("By chcecking that the CronJob has one active Job")
			Eventually(func(g Gomega) {
				g.Expect(k8sClient.Get(ctx, cronjobLookupKey, createdCronjob)).To(Succeed(), "should GET the CronJob")
				g.Expect(createdCronjob.Status.Active).To(HaveLen(1), "should have exactly one active job")
				g.Expect(createdCronjob.Status.Active[0].Name).To(Equal(JobName), "the wrong job is active")
			}, timeout, interval).Should(Succeed(), "should list our active job %s in the active jobs list in status", JobName)

			By("By checking that the CronJob status conditions are properply set")
			Eventually(func(g Gomega) {
				g.Expect(k8sClient.Get(ctx, cronjobLookupKey, createdCronjob)).To(Succeed())
				g.Expect(hasCondition(createdCronjob.Status.Conditions, "Available", metav1.ConditionTrue)).To(BeTrue(),
					"CronJob should have Availalbe condition set to True")
			}, timeout, interval).Should(Succeed())
		})
	})
})
