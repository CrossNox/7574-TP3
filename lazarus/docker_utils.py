from dataclasses import dataclass
from typing import Dict, List, Union, Optional

import docker
from lazarus.cfg import cfg
from lazarus.utils import get_logger, queue_in_name
from lazarus.constants import DOCKER_NETWORK, DEFAULT_DATA_DIR, DOCKER_IMAGE_NAME

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
            volumes=[
                f"{cfg.lazarus.datadir()}:{DEFAULT_DATA_DIR}",
            ],
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
        if self.container:
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
    config = cfg.to_dict()

    for k, v in config.items():
        if k.startswith("group"):
            group_id = k[len("group_") :]
            n_replicas = int(v["replicas"])

            input_group = v["input_group"]
            input_group_size: int
            if input_group == "client":
                input_group_size = 1
                input_group = v["input_queue"]
            else:
                input_group_size = int(config[f"group_{input_group}"]["replicas"])

            input_group_arg = f"--input-group {input_group}:{input_group_size}"

            output_groups = [x for x in v["output_groups"].split(" ") if x != ""]
            output_groups_sizes = (
                []
                if len(output_groups) == 0
                else [config[f"group_{g}"]["replicas"] for g in output_groups]
            )
            if int(v["dummy_out"]):
                output_groups.append("dummy")
                output_groups_sizes.append("1")

            output_groups_arg = " ".join(
                f"--output-groups {':'.join(x)}"
                for x in zip(output_groups, output_groups_sizes)
            )

            command = f"{v['command']} {v['subcommand']}"

            for i in range(n_replicas):
                depends_on = " ".join(
                    queue_in_name(arg, group_id, i)
                    for arg in v["depends_on"].split(" ")
                    if arg != ""
                )
                container_command = f"{command} {i} {v['args']} {depends_on} {input_group_arg} {output_groups_arg} --group-id {group_id}"
                containers.append(SystemContainer(container_command, f"{group_id}_{i}"))

    logger.info("Parsed %s system containers", len(containers))
    logger.info("Parsed containers: %s", containers)
    return containers
