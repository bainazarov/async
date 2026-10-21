from __future__ import annotations


class CrawlerError(Exception):
    """Базовый класс всех ошибок краулера."""


class TransientError(CrawlerError):

    def __init__(self, message: str, url: str = "", status: int | None = None,
                 suggested_delay: float | None = None):
        super().__init__(message)
        self.url = url
        self.status = status
        # сервер может подсказать, через сколько секунд повторить (Retry-After)
        self.suggested_delay = suggested_delay


class PermanentError(CrawlerError):

    def __init__(self, message: str, url: str = "", status: int | None = None):
        super().__init__(message)
        self.url = url
        self.status = status


class NetworkError(CrawlerError):

    def __init__(self, message: str, url: str = ""):
        super().__init__(message)
        self.url = url


class ParseError(CrawlerError):

    def __init__(self, message: str, url: str = ""):
        super().__init__(message)
        self.url = url


class CircuitOpenError(PermanentError):
    """Circuit breaker для домена открыт — запросы временно не отправляются.

    Наследуется от PermanentError, чтобы стратегия повторов не пыталась
    пробивать открытый «прерыватель» бесконечными повторами.
    """


def error_from_status(status: int, url: str,
                      retry_after: float | None = None) -> CrawlerError:
    if status == 429:
        return TransientError(
            f"429 Too Many Requests: {url}", url=url, status=status,
            suggested_delay=retry_after)
    if status >= 500:
        return TransientError(
            f"HTTP {status} для {url}", url=url, status=status,
            suggested_delay=retry_after)
    if status >= 400:
        return PermanentError(
            f"HTTP {status} для {url}", url=url, status=status)
    return PermanentError(
        f"Неожиданный HTTP-статус {status} для {url}", url=url, status=status)
