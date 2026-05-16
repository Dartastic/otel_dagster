// Licensed under the Apache License, Version 2.0
// Copyright 2025, Mindful Software LLC, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';
import 'package:dartastic_opentelemetry/testing.dart';
import 'package:otel_dagster/otel_dagster.dart';
import 'package:test/test.dart';

void main() {
  late TestHarness harness;
  late InMemorySpanExporter spans;

  setUpAll(() async {
    harness = await maybeInitializeOtelForTest(
      serviceName: 'otel_dagster-test',
    );
    spans = harness.spans;
  });

  setUp(() => harness.clear());

  Map<String, Object> attrsOf(Span s) =>
      {for (final a in s.attributes.toList()) a.key: a.value};

  test('assets.materialize records asset + partition + checks', () async {
    await tracedDagster<String>(
      operation: 'assets.materialize',
      invoke: () async => const DagsterCallResult(
        value: 'ok',
        result: DagsterResult(
          deploymentName: 'prod',
          codeLocation: 'analytics_etl',
          assetKey: 'analytics/users_daily',
          assetGroup: 'core_metrics',
          runId: 'run_1',
          runStatus: 'SUCCESS',
          partition: '2026-05-14',
          materializations: 1,
          assetChecksPassed: 4,
          assetChecksFailed: 0,
          durationMs: 14500,
          statusCode: 200,
        ),
      ),
    );
    final a = attrsOf(spans.findSpanByName('dagster assets.materialize')!);
    expect(a['dagster.deployment.name'], 'prod');
    expect(a['dagster.asset.key'], 'analytics/users_daily');
    expect(a['dagster.partition'], '2026-05-14');
    expect(a['dagster.run.status'], 'SUCCESS');
    expect(a['dagster.asset_checks.passed'], 4);
  });

  test('FAILURE run sets span error', () async {
    await tracedDagster<String>(
      operation: 'launchPipelineExecution',
      invoke: () async => const DagsterCallResult(
        value: 'ok',
        result: DagsterResult(
          jobName: 'load_warehouse',
          runId: 'run_2',
          runStatus: 'FAILURE',
        ),
      ),
    );
    expect(
      spans.findSpanByName('dagster launchPipelineExecution')!.status,
      SpanStatusCode.Error,
    );
  });

  test('asset_checks failed > 0 sets span error', () async {
    await tracedDagster<String>(
      operation: 'assets.materialize',
      invoke: () async => const DagsterCallResult(
        value: 'ok',
        result: DagsterResult(
          assetKey: 'finance/revenue',
          runStatus: 'SUCCESS',
          assetChecksPassed: 2,
          assetChecksFailed: 1,
        ),
      ),
    );
    expect(
      spans.findSpanByName('dagster assets.materialize')!.status,
      SpanStatusCode.Error,
    );
  });

  test('suppression skips spans', () async {
    await runWithoutDagsterInstrumentationAsync(() async {
      await tracedDagster<String>(
        operation: 'runs.list',
        invoke: () async => const DagsterCallResult(value: 'v'),
      );
    });
    expect(spans.findSpansStartingWith('dagster '), isEmpty);
  });
}
