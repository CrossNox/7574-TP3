from dataclasses import dataclass
from typing import Dict, List, Union, Optional

import docker
from lazarus.cfg import cfg
from lazarus.utils import get_logger
from lazarus.constants import DOCKER_NETWORK, DOCKER_IMAGE_NAME

logger = get_logger(__name__)


def revive(
    identifier: str,
    command: Union[str, List[str]],
    image=cfg.lazarus.docker_image(default=DOCKER_IMAGE_NAME),
    network=cfg.lazarus.docker_network(default=DOCKER_NETWORK),
    env: Optional[Dict] = None,
):
    env = env or {}
    env["IDENTIFIER"] = identifier
    try:
        docker_client = docker.from_env()
        container = docker_client.containers.run(
            image,
            name=identifier,
            command=command,
            detach=True,
            network=network,
            environment=env,
            remove=True,
        )
        return container
    except docker.errors.ImageNotFound:
        logger.error(
            "Image %s was not found, please check configuration", image, exc_info=True
        )
    except docker.errors.APIError:
        logger.error("Error from the docker server", exc_info=True)
    except docker.errors.ContainerError:
        logger.error("Error in container", exc_info=True)


@dataclass
class SystemContainer:
    command: Union[str, List[str]]
    identifier: str

    def __post_init__(self):
        self.container = None

    def revive(self):
        self.container = revive(self.identifier, self.command)

    def __del__(self):
        self.container.remove()

    def heartbeat_callback(self, host: str, port: int):
        _, _ = host, port
        self.revive()

    def __str__(self):
        return f"SystemContainer {self.identifier} running {self.command}"

    def __repr__(self):
        return self.__str__()


def list_containers_from_config() -> List[SystemContainer]:
    containers = []
    for k, v in cfg.to_dict().items():
        if "container" in k:
            for i in range(int(v["replicas"])):
                containers.append(
                    SystemContainer(v["command"], v["identifier"].format(id=i))
                )
    logger.info("Parsed %s system containers", len(containers))
    logger.info("Parsed containers: %s", containers)
    return containers
