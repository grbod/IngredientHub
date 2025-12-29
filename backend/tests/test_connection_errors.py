"""
Tests for connection error detection logic.
Used by DatabaseConnection wrapper for auto-reconnect.
"""
import pytest


class TestDatabaseConnectionErrorDetection:
    """Test is_connection_error detection across scrapers."""

    def test_connection_closed_error(self):
        """Detect 'connection already closed' errors."""
        from bulksupplements_scraper import DatabaseConnection

        db = DatabaseConnection()
        error = Exception("connection already closed")
        assert db.is_connection_error(error) is True

    def test_connection_is_closed_error(self):
        """Detect 'connection is closed' errors."""
        from boxnutra_scraper import DatabaseConnection

        db = DatabaseConnection()
        error = Exception("connection is closed")
        assert db.is_connection_error(error) is True

    def test_server_closed_connection_error(self):
        """Detect 'server closed the connection' errors."""
        from trafapharma_scraper import DatabaseConnection

        db = DatabaseConnection()
        error = Exception("server closed the connection unexpectedly")
        assert db.is_connection_error(error) is True

    def test_could_not_receive_data_error(self):
        """Detect 'could not receive data' errors."""
        from bulksupplements_scraper import DatabaseConnection

        db = DatabaseConnection()
        error = Exception("could not receive data from server")
        assert db.is_connection_error(error) is True

    def test_ssl_syscall_error(self):
        """Detect SSL syscall errors."""
        from bulksupplements_scraper import DatabaseConnection

        db = DatabaseConnection()
        error = Exception("SSL SYSCALL error: EOF detected")
        assert db.is_connection_error(error) is True

    def test_operation_timed_out_error(self):
        """Detect 'operation timed out' errors."""
        from bulksupplements_scraper import DatabaseConnection

        db = DatabaseConnection()
        error = Exception("operation timed out")
        assert db.is_connection_error(error) is True

    def test_connection_refused_error(self):
        """Detect 'connection refused' errors."""
        from bulksupplements_scraper import DatabaseConnection

        db = DatabaseConnection()
        error = Exception("connection refused")
        assert db.is_connection_error(error) is True

    def test_connection_reset_error(self):
        """Detect 'connection reset' errors."""
        from bulksupplements_scraper import DatabaseConnection

        db = DatabaseConnection()
        error = Exception("connection reset by peer")
        assert db.is_connection_error(error) is True

    def test_broken_pipe_error(self):
        """Detect 'broken pipe' errors."""
        from bulksupplements_scraper import DatabaseConnection

        db = DatabaseConnection()
        error = Exception("broken pipe")
        assert db.is_connection_error(error) is True

    def test_network_unreachable_error(self):
        """Detect 'network is unreachable' errors."""
        from bulksupplements_scraper import DatabaseConnection

        db = DatabaseConnection()
        error = Exception("network is unreachable")
        assert db.is_connection_error(error) is True

    def test_non_connection_error(self):
        """Non-connection errors return False."""
        from bulksupplements_scraper import DatabaseConnection

        db = DatabaseConnection()

        # SQL errors are not connection errors
        assert db.is_connection_error(Exception("syntax error")) is False
        assert db.is_connection_error(Exception("duplicate key")) is False
        assert db.is_connection_error(Exception("foreign key violation")) is False
        assert db.is_connection_error(Exception("table does not exist")) is False

    def test_case_insensitive_detection(self):
        """Error detection is case-insensitive."""
        from bulksupplements_scraper import DatabaseConnection

        db = DatabaseConnection()

        assert db.is_connection_error(Exception("CONNECTION ALREADY CLOSED")) is True
        assert db.is_connection_error(Exception("Connection Reset")) is True
        assert db.is_connection_error(Exception("BROKEN PIPE")) is True

    def test_partial_match_detection(self):
        """Errors containing connection keywords are detected."""
        from bulksupplements_scraper import DatabaseConnection

        db = DatabaseConnection()

        # Full error messages with additional context
        error = Exception("psycopg2.OperationalError: connection already closed")
        assert db.is_connection_error(error) is True

        error = Exception("Error during query: server closed the connection unexpectedly")
        assert db.is_connection_error(error) is True


class TestDatabaseConnectionConsistency:
    """Ensure all scrapers have consistent error detection."""

    def test_all_scrapers_detect_same_errors(self):
        """All scrapers detect the same connection errors."""
        from bulksupplements_scraper import DatabaseConnection as BSConn
        from boxnutra_scraper import DatabaseConnection as BNConn
        from trafapharma_scraper import DatabaseConnection as TPConn

        test_errors = [
            "connection already closed",
            "server closed the connection",
            "ssl syscall error",
            "operation timed out",
            "connection refused",
        ]

        bs_db = BSConn()
        bn_db = BNConn()
        tp_db = TPConn()

        for error_msg in test_errors:
            error = Exception(error_msg)
            bs_result = bs_db.is_connection_error(error)
            bn_result = bn_db.is_connection_error(error)
            tp_result = tp_db.is_connection_error(error)

            assert bs_result == bn_result == tp_result, \
                f"Inconsistent detection for: {error_msg}"
