import random
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int
    base_seconds: float
    max_seconds: float
    jitter_min: float
    jitter_max: float

    def delay_for(self, attempt: int) -> float:
        capped: float = min(self.base_seconds * 2.0 ** (attempt - 1), self.max_seconds)
        return capped * random.uniform(self.jitter_min, self.jitter_max)

    def is_last(self, attempt: int) -> bool:
        return attempt >= self.max_attempts
