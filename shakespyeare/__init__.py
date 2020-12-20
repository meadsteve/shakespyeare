import asyncio
import logging
import random
import uuid
from abc import ABC, abstractmethod
from asyncio import get_running_loop, AbstractEventLoop, Task
from concurrent.futures._base import Executor
from concurrent.futures.thread import ThreadPoolExecutor
from multiprocessing import Value
from multiprocessing.context import Process, SpawnContext
from multiprocessing.queues import Queue
from queue import Empty
from typing import Dict, Optional, Any, List, Iterable
from typing_extensions import Protocol

logger = logging.getLogger(__name__)


class Actor(Protocol):
    name: Optional[str]
    in_queue: Queue
    out_queue: Queue
    alive: Value
    async def runner(self): ...


class ActorRunner:
    def __init__(self, actors: Iterable[Actor]):
        self._process = Process(target=self._run, args=(actors,))
        self._process.start()

    def _run(self, actors: Iterable[Actor]):
        asyncio.run(self._loop(actors))

    async def _loop(self, actors: Iterable[Actor]):
        loop = get_running_loop()
        for actor in actors:
            loop.create_task(actor.runner())
        while loop.is_running():
            await asyncio.sleep(1)


class Stage:
    _actors: Dict[uuid.UUID, Actor]
    _friendly_names: Dict[str, uuid.UUID]
    _runners: List[ActorRunner]

    def __init__(self):
        self._actors = {}
        self._friendly_names = {}
        self.loop = get_running_loop()
        self.loop.create_task(self._dispatch_messages(self.loop))
        self._runners = []

    async def _dispatch_messages(self, loop: AbstractEventLoop):
        while loop.is_running():
            actors = {actor_id: actor for actor_id, actor in self._actors.items() if actor.alive.value}
            for actor_id, actor in actors.items():
                self._fetch_single_message(actor_id, actor.out_queue)
            self._actors = actors
            await asyncio.sleep(0)

    def _fetch_single_message(self, actor_id, queue: Queue):
        try:
            target, message = queue.get_nowait()
            self._dispatch_single_message(target, actor_id, message)
        except Empty:
            logger.debug("nothing to fetch")

    def _dispatch_single_message(self, to, from_id, message):
        if hasattr(to, "id"):
            receiver_id = to.id
        elif isinstance(to, uuid.UUID):
            receiver_id = to
        elif isinstance(to, str) and to in self._friendly_names:
            receiver_id = self._friendly_names[to]
        else:
            logger.warning(f"Invalid message address: {to}")
            return
        self._actors[receiver_id].in_queue.put((from_id, message))

    def add_actors(self, *actors: Actor):
        for actor in actors:
            new_id = uuid.uuid4()
            self._actors[new_id] = actor
            if hasattr(actor, "name") and actor.name:
                self._friendly_names[actor.name] = new_id
        self._runners.append(ActorRunner(actors))

    def send_message(self, to, message):
        self._dispatch_single_message(to, None, message)


class BasicActor(ABC):
    name: Optional[str]
    in_queue: Queue
    out_queue: Queue
    alive: Value
    _loop_task: Optional[Task]

    def __init__(self, name=None):
        self.name = name
        self._state = {}
        ctx = SpawnContext()
        self.alive = Value('b', True)
        self.in_queue = Queue(ctx=ctx)
        self.out_queue = Queue(ctx=ctx)

    async def runner(self):
        loop = get_running_loop()
        self._loop_task = loop.create_task(self._do_loop(loop))

    async def _do_loop(self, loop: AbstractEventLoop):
        while loop.is_running():
            try:
                print("Working...")
                sent_from, message = self.in_queue.get(timeout=0.1)
                loop.create_task(self._handle_message(message, sent_from, self._state))
            except Empty:
                pass
            await asyncio.sleep(0)

    def send_message(self, to, message):
        self.out_queue.put((to, message))

    async def _handle_message(self, message, sent_from, state):
        try:
            self._state = await self.handle_message(message, sent_from, state)
        except:
            self.alive.value = False
            if self._loop_task:
                self._loop_task.cancel()
            raise

    @abstractmethod
    async def handle_message(self, message, sent_from, state) -> Any:
        pass
