// Licensed under the Dartastic Pro Software License Agreement.
// Copyright 2025-2026 Mindful Software LLC d/b/a Dartastic.io, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';

import 'otel_dagster_suppression.dart';

const _tracerName = 'dartastic_otel_libs_dagster';

Tracer _tracer() => OTel.tracerProvider().getTracer(_tracerName);

class DagsterResult {
  const DagsterResult({
    this.deploymentName,
    this.codeLocation,
    this.assetKey,
    this.assetGroup,
    this.jobName,
    this.runId,
    this.runStatus,
    this.partition,
    this.materializations,
    this.assetChecksPassed,
    this.assetChecksFailed,
    this.stepKey,
    this.stepStatus,
    this.durationMs,
    this.statusCode,
  });

  /// Dagster Cloud deployment (e.g., `prod` / `staging`) or OSS
  /// instance name.
  final String? deploymentName;

  /// Code location (formerly "repository") — the Python module Dagster
  /// loads the asset graph from.
  final String? codeLocation;

  /// Asset key as a slash-joined path (e.g., `analytics/users_daily`).
  final String? assetKey;

  /// Logical group the asset belongs to.
  final String? assetGroup;

  /// Job (sensor/schedule target). Null for asset-driven runs.
  final String? jobName;

  final String? runId;

  /// `STARTED` / `SUCCESS` / `FAILURE` / `CANCELED` / `QUEUED` /
  /// `NOT_STARTED` / `MANAGED` / `STARTING` / `CANCELING`.
  final String? runStatus;

  /// Partition key (date string, region, etc.) for partitioned assets.
  final String? partition;

  /// Materializations emitted in this run.
  final int? materializations;

  final int? assetChecksPassed;
  final int? assetChecksFailed;

  final String? stepKey;

  /// `success` / `failure` / `skipped` / `start` / `restarted` —
  /// op/step level.
  final String? stepStatus;

  final int? durationMs;
  final int? statusCode;
}

class DagsterCallResult<T> {
  const DagsterCallResult({required this.value, this.result});
  final T value;
  final DagsterResult? result;
}

/// Wraps a Dagster GraphQL API call in a CLIENT span. [operation] is
/// one of `launchPipelineExecution`, `launchPipelineReexecution`,
/// `launchPartitionBackfill`, `runs.terminate`, `assets.materialize`,
/// `assets.reportRuntime`, `schedules.start`, `sensors.start`,
/// `runs.list`, `assets.list`.
Future<T> tracedDagster<T>({
  required String operation,
  required Future<DagsterCallResult<T>> Function() invoke,
}) async {
  if (dagsterInstrumentationSuppressed()) {
    final r = await invoke();
    return r.value;
  }
  final m = <String, Object>{
    'rpc.system': 'dagster',
    'rpc.method': operation,
    'dagster.operation': operation,
  };

  final span = _tracer().startSpan(
    'dagster $operation',
    kind: SpanKind.client,
    attributes: OTel.attributesFromMap(m),
  );
  try {
    final r = await invoke();
    if (r.result != null) {
      final extras = <Attribute<Object>>[];
      if (r.result!.deploymentName != null) {
        extras.add(OTel.attributeString(
          'dagster.deployment.name',
          r.result!.deploymentName!,
        ));
      }
      if (r.result!.codeLocation != null) {
        extras.add(OTel.attributeString(
          'dagster.code_location',
          r.result!.codeLocation!,
        ));
      }
      if (r.result!.assetKey != null) {
        extras.add(OTel.attributeString(
          'dagster.asset.key',
          r.result!.assetKey!,
        ));
      }
      if (r.result!.assetGroup != null) {
        extras.add(OTel.attributeString(
          'dagster.asset.group',
          r.result!.assetGroup!,
        ));
      }
      if (r.result!.jobName != null) {
        extras.add(OTel.attributeString(
          'dagster.job.name',
          r.result!.jobName!,
        ));
      }
      if (r.result!.runId != null) {
        extras.add(OTel.attributeString(
          'dagster.run.id',
          r.result!.runId!,
        ));
      }
      if (r.result!.runStatus != null) {
        extras.add(OTel.attributeString(
          'dagster.run.status',
          r.result!.runStatus!,
        ));
        if (r.result!.runStatus == 'FAILURE') {
          span.setStatus(SpanStatusCode.Error, 'Dagster run FAILURE');
        }
      }
      if (r.result!.partition != null) {
        extras.add(OTel.attributeString(
          'dagster.partition',
          r.result!.partition!,
        ));
      }
      if (r.result!.materializations != null) {
        extras.add(OTel.attributeInt(
          'dagster.materializations',
          r.result!.materializations!,
        ));
      }
      if (r.result!.assetChecksPassed != null) {
        extras.add(OTel.attributeInt(
          'dagster.asset_checks.passed',
          r.result!.assetChecksPassed!,
        ));
      }
      if (r.result!.assetChecksFailed != null) {
        extras.add(OTel.attributeInt(
          'dagster.asset_checks.failed',
          r.result!.assetChecksFailed!,
        ));
        if (r.result!.assetChecksFailed! > 0) {
          span.setStatus(
            SpanStatusCode.Error,
            'Dagster asset checks failed',
          );
        }
      }
      if (r.result!.stepKey != null) {
        extras.add(OTel.attributeString(
          'dagster.step.key',
          r.result!.stepKey!,
        ));
      }
      if (r.result!.stepStatus != null) {
        extras.add(OTel.attributeString(
          'dagster.step.status',
          r.result!.stepStatus!,
        ));
      }
      if (r.result!.durationMs != null) {
        extras.add(OTel.attributeInt(
          'dagster.duration_ms',
          r.result!.durationMs!,
        ));
      }
      if (r.result!.statusCode != null) {
        extras.add(OTel.attributeInt(
          'http.response.status_code',
          r.result!.statusCode!,
        ));
        if (r.result!.statusCode! >= 400) {
          span.setStatus(
            SpanStatusCode.Error,
            'Dagster HTTP ${r.result!.statusCode}',
          );
        }
      }
      if (extras.isNotEmpty) span.addAttributes(OTel.attributes(extras));
    }
    return r.value;
  } catch (e, st) {
    span.addAttributes(OTel.attributes([
      OTel.attributeString(
        ErrorResource.errorType.key,
        e.runtimeType.toString(),
      ),
    ]));
    span.recordException(e, stackTrace: st);
    span.setStatus(SpanStatusCode.Error, e.toString());
    rethrow;
  } finally {
    span.end();
  }
}
