"""
Tests for master-replica replication.

These tests start a master and multiple replicas, write data to the master,
and verify it appears on all replicas.
"""

import time

import pytest
import redis


# How long to wait for replication propagation
REPLICATION_PROPAGATION_WAIT = 1.0


class TestReplicaHandshake:
    """Test that replicas successfully connect to the master."""

    def test_replica_responds_to_ping(self, replica_servers):
        for replica in replica_servers:
            client = replica.client()
            assert client.ping() is True

    def test_replica_info_shows_slave_role(self, replica_servers):
        for replica in replica_servers:
            client = replica.client()
            info = client.execute_command("INFO", "replication")
            if isinstance(info, dict):
                assert info.get("role") == "slave"
            else:
                assert "role:slave" in info

    def test_master_info_shows_master_role(self, master_server, replica_servers):
        # replica_servers fixture ensures replicas are connected
        client = master_server.client()
        info = client.execute_command("INFO", "replication")
        if isinstance(info, dict):
            assert info.get("role") == "master"
        else:
            assert "role:master" in info


class TestWritePropagation:
    """Test that SET commands on master propagate to replicas."""

    def test_single_key_propagates(self, master_server, replica_servers):
        master_client = master_server.client()
        master_client.set("replicated_key", "replicated_value")

        time.sleep(REPLICATION_PROPAGATION_WAIT)

        for replica in replica_servers:
            client = replica.client()
            result = client.get("replicated_key")
            assert result == "replicated_value", (
                f"Replica on port {replica.port} did not have the expected value. "
                f"Got: {result!r}"
            )

    def test_multiple_keys_propagate(self, master_server, replica_servers):
        master_client = master_server.client()

        test_data = {
            "user:1": "Alice",
            "user:2": "Bob",
            "user:3": "Charlie",
            "counter": "42",
            "status": "active",
        }

        for key, value in test_data.items():
            master_client.set(key, value)

        time.sleep(REPLICATION_PROPAGATION_WAIT)

        for replica in replica_servers:
            client = replica.client()
            for key, expected_value in test_data.items():
                result = client.get(key)
                assert result == expected_value, (
                    f"Replica on port {replica.port}: "
                    f"key={key!r} expected={expected_value!r} got={result!r}"
                )

    def test_overwrite_propagates(self, master_server, replica_servers):
        master_client = master_server.client()

        master_client.set("mutable_key", "initial")
        time.sleep(REPLICATION_PROPAGATION_WAIT)

        # Verify initial value on replicas
        for replica in replica_servers:
            assert replica.client().get("mutable_key") == "initial"

        # Overwrite on master
        master_client.set("mutable_key", "updated")
        time.sleep(REPLICATION_PROPAGATION_WAIT)

        # Verify updated value on replicas
        for replica in replica_servers:
            result = replica.client().get("mutable_key")
            assert result == "updated", (
                f"Replica on port {replica.port}: "
                f"expected 'updated', got {result!r}"
            )

    def test_sequential_writes_propagate_in_order(self, master_server, replica_servers):
        master_client = master_server.client()

        # Write the same key multiple times
        for i in range(5):
            master_client.set("seq_key", f"version_{i}")

        time.sleep(REPLICATION_PROPAGATION_WAIT)

        # All replicas should have the final version
        for replica in replica_servers:
            result = replica.client().get("seq_key")
            assert result == "version_4", (
                f"Replica on port {replica.port}: "
                f"expected 'version_4', got {result!r}"
            )

    def test_many_keys_propagate(self, master_server, replica_servers):
        """Stress test: write many keys and verify all replicate."""
        master_client = master_server.client()

        num_keys = 50
        for i in range(num_keys):
            master_client.set(f"bulk_key_{i}", f"bulk_value_{i}")

        time.sleep(REPLICATION_PROPAGATION_WAIT * 2)

        for replica in replica_servers:
            client = replica.client()
            for i in range(num_keys):
                result = client.get(f"bulk_key_{i}")
                assert result == f"bulk_value_{i}", (
                    f"Replica on port {replica.port}: "
                    f"bulk_key_{i} expected 'bulk_value_{i}', got {result!r}"
                )


class TestReplicationWithExpiry:
    """Test that SET with PX expiration replicates correctly."""

    def test_expiring_key_propagates(self, master_server, replica_servers):
        master_client = master_server.client()

        # Set a key with 5-second expiry on master
        master_client.set("expiring_replicated", "temp_value", px=5000)

        time.sleep(REPLICATION_PROPAGATION_WAIT)

        # Key should exist on replicas
        for replica in replica_servers:
            result = replica.client().get("expiring_replicated")
            assert result == "temp_value", (
                f"Replica on port {replica.port}: "
                f"expected 'temp_value', got {result!r}"
            )

    def test_non_expiring_key_persists_on_replicas(self, master_server, replica_servers):
        master_client = master_server.client()
        master_client.set("permanent_replicated", "forever")

        time.sleep(REPLICATION_PROPAGATION_WAIT)

        # Wait a bit more and verify key still exists
        time.sleep(1.0)

        for replica in replica_servers:
            result = replica.client().get("permanent_replicated")
            assert result == "forever"


class TestMasterReadAfterReplicaSetup:
    """Ensure the master still works normally after replicas connect."""

    def test_master_get_works(self, master_server, replica_servers):
        client = master_server.client()
        client.set("master_test", "master_value")
        assert client.get("master_test") == "master_value"

    def test_master_ping_works(self, master_server, replica_servers):
        client = master_server.client()
        assert client.ping() is True

    def test_master_echo_works(self, master_server, replica_servers):
        client = master_server.client()
        assert client.echo("test") == "test"
