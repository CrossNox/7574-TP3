# Note: This is a mock class only used to provide an interface
from typing import Optional


class LeaderElectionMock:
    def wait_for_leader(self) -> bool:
        return True

    def i_am_leader(self) -> bool:
        return True

    def get_leader(self) -> Optional[str]:
        return None
