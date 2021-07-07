from asyncio import AbstractEventLoop, Event, Lock
from io import IOBase

from aiohttp import ClientSession, ClientResponse, FormData


BASE_URL = "https://discord.com/api/v9"


class HTTPError(Exception):
    def __init__(self, response: ClientResponse, *args, **kwargs) -> None:
        self.response = response
        super().__init__(*args, **kwargs)


class File:
    def __init__(self, file: IOBase, filename: str) -> None:
        """A file object representing Discord files.

        Args:
            file (IOBase): The file to upload.
            filename (str): The filename to use.
        """

        self.file = file
        self.filename = filename


class HTTPClient:
    def __init__(self, token: str, url: str, loop: AbstractEventLoop) -> None:
        """An HTTP client to make ratelimited API requests.

        Args:
            token (str): The API token to authenticate requests with.
            url (str): The API base URL.
            loop (AbstractEventLoop): The asyncio event loop to use.
        """

        self._token = token
        self._url = url
        self._loop = loop

        self._buckets = {}
        self._global = Event()
        self._global.set()

        self._raw_session = None

    @property
    def _session(self) -> ClientSession:
        if self._raw_session is None or self._raw_session.closed:
            self._raw_session = ClientSession(headers={
                "Authorization": f"Bot {self._token}",
                "User-Agent": f"DiscordBot (microcord https://github.com/vcokltfre/microcord)",
                "X-RateLimit-Precision": "millisecond",
            })
        return self._raw_session

    @staticmethod
    def _get_bucket(path: str, params: dict) -> str:
        cid = params.get("channel_id", "")
        gid = params.get("guild_id", "")
        wid = params.get("webhook_id", "")

        return f"{path}/{cid}:{gid}:{wid}"

    def _get_lock(self, bucket: str) -> Lock:
        lock = self._buckets.get(bucket)
        if not lock:
            lock = Lock(loop=self._loop)
            self._buckets[bucket] = lock
        return lock

    async def _acquire(self, bucket: str) -> None:
        lock = self._get_lock(bucket)
        await lock.acquire()
        await self._global.wait()

    def _lock_global(self, unlock_after: float) -> None:
        self._global.clear()
        self._loop.call_later(unlock_after, self._global.set)

    def _unlock(self, bucket: str, delay: float) -> None:
        lock = self._get_lock(bucket)
        if lock.locked():
            self._loop.call_later(delay, lock.release)

    async def request(self, method: str, path: str, path_params: dict = None, retries: int = 3, **kwargs) -> ClientResponse:
        """Make a ratelimited request to the Discord API.

        Args:
            method (str): The HTTP method to use.
            path (str): The request path.
            path_params (dict, optional): The path parameters. Defaults to None.
            retries (int, optional): How many attempts to make before giving up. Defaults to 3.

        Raises:
            HTTPError: Somethign went wrong with the request.

        Returns:
            ClientResponse: The request response.
        """
        bucket = self._get_bucket(path, path_params)

        request_headers = {}
        if "reason" in kwargs:
            request_headers["X-Audit-Log-Reason"] = kwargs.pop("reason")

        for i in range(retries):
            await self._acquire(bucket)
            rl_sleep_for = 0
            try:
                if files := kwargs.pop("files", []):
                    if i:
                        for file in files:
                            file.seek(0)
                    formdata = FormData()

                    for fn, file in enumerate(files):
                        if not isinstance(file, File):
                            raise TypeError(
                                f"files must be a list of microcord.File, not {file.__class__.__qualname__}"
                            )
                        formdata.add_field(f"file_{fn}", file.file, filename=file.filename)

                    for k, v in kwargs.pop("json", {}).items():
                        formdata.add_field(k, v)

                    kwargs["data"] = formdata
                response = await self._session.request(method, self._url + path.format(**path_params), headers=request_headers, **kwargs)

                status = response.status
                headers = response.headers

                rl_reset_after = float(headers.get("X-RateLimit-Reset-After", 0))
                rl_remaining = float(headers.get("X-RateLimit-Remaining", 1))

                if status != 429 and rl_remaining == 0:
                    rl_sleep_for = rl_reset_after

                if 200 <= status < 300:
                    return response

                if status == 429:
                    if not headers.get("Via"):
                        raise HTTPError(response)

                    data = await response.json()
                    is_global = data.get("global", False)
                    rl_sleep_for = data.get("retry_after")

                    if is_global:
                        self._lock_global(rl_sleep_for)

                    elif status >= 500:
                        rl_sleep_for = 1 + i * 2

                    if i == retries - 1:
                        rl_sleep_for = 0
                        continue
            finally:
                self._unlock(bucket, rl_sleep_for)

    async def close(self) -> None:
        """Close the session gracefully."""

        await self._raw_session.close()
