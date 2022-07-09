from dataclasses import dataclass
from typing import Dict, List, Union, Optional

import docker

from lazarus.cfg import cfg
from lazarus.constants import (
    DOCKER_NETWORK,
    DEFAULT_DATA_DIR,
    DOCKER_IMAGE_NAME,
    DEFAULT_HEARTBEAT_PORT,
)
from lazarus.utils import get_logger, queue_in_name

logger = get_logger(__name__)


def revive(
    identifier: str,
    command: Union[str, List[str]],
    image=cfg.lazarus.docker_image(default=DOCKER_IMAGE_NAME),
    network=cfg.lazarus.docker_network(default=DOCKER_NETWORK),
    env: Optional[Dict] = None,
    group: Optional[str] = None,
    group_ids: List[str] = [],
    **kwargs,
):
    logger.info("Coordinator reviving %s", identifier)
    env = env or {}
    env["LAZARUS_IDENTIFIER"] = identifier
    env["LAZARUS_GROUP"] = group
    env["LAZARUS_GROUPIDS"] = " ".join(group_ids)
    env.update(kwargs)
    try:
        docker_client = docker.from_env()
        container = docker_client.containers.run(
            image,
            name=identifier,
            command=command,
            detach=True,
            network=network,
            environment=env,
            volumes=[f"{cfg.lazarus.datadir()}:{DEFAULT_DATA_DIR}",],
            remove=True,
        )

        import zmq

        from lazarus.constants import HEARTBEAT

        ctx = zmq.Context.instance()
        sub = ctx.socket(zmq.SUB)
        sub.setsockopt_string(zmq.SUBSCRIBE, "")
        sub.connect(f"tcp://{identifier}:{DEFAULT_HEARTBEAT_PORT}")
        hb = sub.recv_json()
        if hb["node_id"] == identifier and hb["payload"] == HEARTBEAT:  # type: ignore
            return container
        else:
            raise ValueError("Docker failed!")
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
    group: str
    group_ids: List[str]

    def __post_init__(self):
        self.container = None

    def revive(self):
        self.container = revive(
            self.identifier,
            self.command,
            group=self.group,
            group_ids=self.group_ids,
            LAZARUS_SERVERS=cfg.lazarus.servers(),
        )

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

            input_group_arg = ""

            for group in v["input_group"].split(" "):
                group_size: int
                if group == "client":
                    group_size = 1
                    group = v["input_queue"]
                else:
                    group_size = int(config[f"group_{group}"]["replicas"])

                input_group_arg += f" --input-group {group}:{group_size}"

            output_groups = [x for x in v["output_groups"].split(" ") if x != ""]
            to_server = "servers" in output_groups
            output_groups = [x for x in output_groups if x != "servers"]

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

            if to_server:
                output_groups_arg += " --output-groups servers"

            command = f"{v['command']} {v['subcommand']}"

            group_ids = [f"{group_id}_{i}" for i in range(n_replicas)]
            for i, identifier in enumerate(group_ids):
                depends_on = " ".join(
                    queue_in_name(arg, group_id, i)
                    for arg in v["depends_on"].split(" ")
                    if arg != ""
                )
                container_command = f"{command} {i} {v['args']} {depends_on} {input_group_arg} {output_groups_arg} --group-id {group_id}"
                containers.append(
                    SystemContainer(container_command, identifier, group_id, group_ids)
                )

    logger.info(
        "Parsed %d containers: %s", len(containers), [c.identifier for c in containers]
    )
    return containers
