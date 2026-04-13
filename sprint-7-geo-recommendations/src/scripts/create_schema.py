"""
Script for creating the Data Lake structure in HDFS on the remote server.
Creates directories according to the architecture described in README.md.
Connects to the remote server over SSH and runs commands inside the Docker container.
"""

import subprocess
import logging
import os
from typing import List, Tuple

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HDFSSchemaCreator:
    """Class for creating the Data Lake structure in HDFS on the remote server."""

    def __init__(
        self,
        base_path: str = "/user/student/project/geo",
        remote_host: str = "10.0.0.11",
        remote_user: str = "cluster-user",
        ssh_key_path: str = "~/.ssh/ssh_private_key",
        container_name: str = None
    ):
        """
        Initialize the schema creator.

        Args:
            base_path: Base HDFS path for the project
            remote_host: IP address of the remote server
            remote_user: User on the remote server
            ssh_key_path: Path to the SSH key
            container_name: Docker container name (if None - detected automatically)
        """
        self.base_path = base_path
        self.remote_host = remote_host
        self.remote_user = remote_user
        self.ssh_key_path = os.path.expanduser(ssh_key_path)
        self.container_name = container_name
        self.directories = self._define_directories()

    def _define_directories(self) -> List[Tuple[str, str]]:
        """
        Defines the list of directories to create.

        Returns:
            List of tuples (path, description)
        """
        return [
            # RAW Layer
            (f"{self.base_path}/raw", "RAW Layer - raw data"),
            (f"{self.base_path}/raw/events", "Events with coordinates"),
            (f"{self.base_path}/raw/geo_csv", "Australian cities dictionary"),

            # ODS Layer
            (f"{self.base_path}/ods", "ODS Layer - cleaned data"),
            (f"{self.base_path}/ods/events", "Cleaned events"),
            (f"{self.base_path}/ods/geo", "Cleaned city coordinates"),

            # DDS Layer
            (f"{self.base_path}/dds", "DDS Layer - integrated data"),
            (f"{self.base_path}/dds/users", "User profiles with home region"),
            (f"{self.base_path}/dds/cities", "City dictionary"),
            (f"{self.base_path}/dds/events_enriched", "Events with geo data"),

            # Analytics/Sandbox Layer
            (f"{self.base_path}/analytics", "Analytics Layer - sandbox for experiments"),
            (f"{self.base_path}/analytics/temp", "Temporary tables"),
            (f"{self.base_path}/analytics/samples", "Samples for testing"),

            # MART Layer
            (f"{self.base_path}/mart", "MART Layer - data marts"),
            (f"{self.base_path}/mart/friend_recommendations", "Friend recommendations"),
            (f"{self.base_path}/mart/user_geo_report", "User geo analytics"),
        ]

    def _get_containers_list(self) -> List[str]:
        """
        Returns the list of all running Docker containers.

        Returns:
            List of container names
        """
        ssh_command = [
            'ssh',
            '-i', self.ssh_key_path,
            '-o', 'StrictHostKeyChecking=no',
            '-o', 'UserKnownHostsFile=/dev/null',
            '-o', 'LogLevel=ERROR',
            f'{self.remote_user}@{self.remote_host}',
            'docker ps --format "{{.Names}}"'
        ]

        try:
            result = subprocess.run(
                ssh_command,
                capture_output=True,
                text=True,
                check=False,
                timeout=10
            )
            if result.returncode == 0 and result.stdout.strip():
                containers = [c.strip() for c in result.stdout.strip().split('\n') if c.strip()]
                return containers
            return []
        except Exception as e:
            logger.error(f"Error fetching container list: {e}")
            return []

    def _detect_container_name(self) -> str:
        """
        Automatically detects the Docker container name with HDFS namenode.

        Returns:
            Container name or None if not found
        """
        logger.info("Fetching the list of running containers...")
        containers = self._get_containers_list()

        if not containers:
            logger.error("No running containers found")
            return None

        logger.info(f"Containers found: {len(containers)}")
        for container in containers:
            logger.info(f"  - {container}")

        # Look for a container with student in the name
        for container in containers:
            if 'student' in container.lower():
                logger.info(f"Selected HDFS container: {container}")
                return container

        # If student was not found, look for namenode
        for container in containers:
            if 'namenode' in container.lower():
                logger.warning(f"Container with 'student' not found, using namenode: {container}")
                return container

        # If neither was found, take the first container
        if containers:
            logger.warning(f"Container with 'student' or 'namenode' not found, using the first one: {containers[0]}")
            return containers[0]

        logger.error("Failed to detect the container")
        return None

    def _run_remote_hdfs_command(self, hdfs_command: str) -> Tuple[bool, str]:
        """
        Runs an HDFS command on the remote server via SSH and Docker.

        Args:
            hdfs_command: HDFS command to execute

        Returns:
            Tuple (success, output)
        """
        # Detect container name if not provided
        if self.container_name is None:
            self.container_name = self._detect_container_name()
            if self.container_name is None:
                logger.error("Failed to detect the container name")
                return False, "Container not found"

        # Command to run the HDFS command inside the Docker container
        docker_command = f"docker exec {self.container_name} {hdfs_command}"

        # SSH command
        ssh_command = [
            'ssh',
            '-i', self.ssh_key_path,
            '-o', 'StrictHostKeyChecking=no',
            '-o', 'UserKnownHostsFile=/dev/null',
            '-o', 'LogLevel=ERROR',
            f'{self.remote_user}@{self.remote_host}',
            docker_command
        ]

        try:
            result = subprocess.run(
                ssh_command,
                capture_output=True,
                text=True,
                check=False,
                timeout=30
            )
            return result.returncode == 0, result.stdout + result.stderr
        except subprocess.TimeoutExpired:
            logger.error("Command execution timed out")
            return False, "Timeout"
        except Exception as e:
            logger.error(f"Command execution error: {e}")
            return False, str(e)

    def _directory_exists(self, path: str) -> bool:
        """
        Checks whether a directory exists in HDFS.

        Args:
            path: Directory path

        Returns:
            True if the directory exists
        """
        success, _ = self._run_remote_hdfs_command(f'hdfs dfs -test -d {path}')
        return success

    def create_directory(self, path: str, description: str) -> bool:
        """
        Creates a directory in HDFS.

        Args:
            path: Directory path
            description: Directory description

        Returns:
            True if the directory is created successfully
        """
        if self._directory_exists(path):
            logger.info(f"Directory already exists: {path}")
            return True

        logger.info(f"Creating directory: {path} ({description})")
        success, output = self._run_remote_hdfs_command(f'hdfs dfs -mkdir -p {path}')

        if success:
            logger.info(f"Created: {path}")
            return True
        else:
            logger.error(f"Failed to create {path}: {output}")
            return False

    def create_schema(self) -> bool:
        """
        Creates the entire Data Lake structure.

        Returns:
            True if all directories are created successfully
        """
        logger.info("=" * 70)
        logger.info("Starting Data Lake structure creation")
        logger.info(f"Base path: {self.base_path}")
        logger.info("=" * 70)

        success_count = 0
        fail_count = 0

        for path, description in self.directories:
            if self.create_directory(path, description):
                success_count += 1
            else:
                fail_count += 1

        logger.info("=" * 70)
        logger.info(f"Finished: {success_count} succeeded, {fail_count} failed")
        logger.info("=" * 70)

        return fail_count == 0

    def verify_schema(self) -> bool:
        """
        Verifies the created structure.

        Returns:
            True if all directories exist
        """
        logger.info("Verifying the created structure...")

        all_exist = True
        for path, description in self.directories:
            exists = self._directory_exists(path)
            status = "OK" if exists else "FAIL"
            logger.info(f"{status} {path}")

            if not exists:
                all_exist = False

        return all_exist

    def show_structure(self) -> None:
        """Displays the Data Lake structure."""
        logger.info("\nPlanned Data Lake structure:\n")

        for path, description in self.directories:
            level = path.replace(self.base_path, "").count("/")
            indent = "  " * (level - 1)
            folder_name = path.split("/")[-1]
            logger.info(f"{indent}  {folder_name}/ - {description}")


def main():
    """Main entry point."""
    logger.info("Connecting to remote server cluster-user@10.0.0.11")
    logger.info("HDFS user: /user/student")

    # Create class instance
    creator = HDFSSchemaCreator(
        base_path="/user/student/project/geo",
        remote_host="10.0.0.11",
        remote_user="cluster-user",
        ssh_key_path="~/.ssh/ssh_private_key",
        container_name=None  # Automatic detection
    )

    # Check SSH key
    if not os.path.exists(creator.ssh_key_path):
        logger.error(f"SSH key not found: {creator.ssh_key_path}")
        return 1

    # Display the planned structure
    creator.show_structure()

    # Create the structure
    success = creator.create_schema()

    # Check the result
    if success:
        logger.info("\nData Lake structure created successfully!")

        # Verification
        if creator.verify_schema():
            logger.info("Verification passed successfully!")
        else:
            logger.warning("Some directories were not found")
    else:
        logger.error("Errors occurred while creating the Data Lake structure")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
