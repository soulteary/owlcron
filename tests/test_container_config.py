"""Offline deployment safety checks; real scheduling still needs a target cluster."""
import json
import pathlib
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]

class DeploymentTests(unittest.TestCase):
    def test_single_writer_and_restricted_workload(self):
        items = json.loads((ROOT / 'deploy/kubernetes/owlcron.json').read_text())['items']
        kinds = {item['kind']: item for item in items}
        deployment = kinds['Deployment']['spec']
        self.assertEqual(deployment['replicas'], 1)
        self.assertEqual(deployment['strategy']['type'], 'Recreate')
        pod = deployment['template']['spec']
        self.assertFalse(pod['automountServiceAccountToken'])
        self.assertTrue(pod['securityContext']['runAsNonRoot'])
        for container in pod['containers'] + pod['initContainers']:
            self.assertTrue(container['securityContext']['readOnlyRootFilesystem'])
            self.assertFalse(container['securityContext']['allowPrivilegeEscalation'])
            self.assertEqual(container['securityContext']['capabilities']['drop'], ['ALL'])
            self.assertIn('secretKeyRef', container['env'][0]['valueFrom'])
        self.assertEqual(kinds['Service']['spec']['type'], 'ClusterIP')
        config = json.loads(kinds['ConfigMap']['data']['config.json'])
        self.assertNotIn('secret_key', config)
        for field in ('storage_base_dir', 'log_dir', 'queue_dir'):
            self.assertTrue(config[field].startswith('/var/lib/owlcron/'))
        self.assertEqual(config, json.loads((ROOT / 'deploy/container-config.json').read_text()))
        self.assertIn('startupProbe', pod['containers'][0])
        self.assertIn('readinessProbe', pod['containers'][0])
        self.assertIn('livenessProbe', pod['containers'][0])

    def test_health_probes_distinguish_readiness_from_liveness(self):
        items = json.loads((ROOT / 'deploy/kubernetes/owlcron.json').read_text())['items']
        workload = next(item for item in items if item['kind'] == 'Deployment')
        container = workload['spec']['template']['spec']['containers'][0]
        for name in ('startupProbe', 'readinessProbe'):
            self.assertEqual(container[name]['httpGet']['path'], '/api/app/ready')
            self.assertGreater(container[name]['timeoutSeconds'], 2)
        self.assertEqual(container['livenessProbe']['httpGet']['path'], '/api/app/ping')
