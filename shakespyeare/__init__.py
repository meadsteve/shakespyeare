import asyncio
import logging
import uuid
from abc import ABC, abstractmethod
from asyncio import get_running_loop, AbstractEventLoop
from collections import defaultdict
from concurrent.futures.thread import ThreadPoolExecutor
from threading import Lock
from typing import Dict, List, Any, Optional
from typing_extensions import Protocol

logger = logging.getLogger(__name__)


class Actor(Protocol):
    def start(self, assigned_id: uuid.UUID): ...

    def handle_message(self, message): ...

    def empty_mailbox(self) -> List: ...

    @property
    def name(self) -> Optional[str]: ...


class Stage:
    _actors: Dict[uuid.UUID, Actor]
    _actor_locks: Dict[uuid.UUID, Lock]
    _mailboxes: Dict[uuid.UUID, List[Any]]
    _mailbox_semaphore: asyncio.Semaphore
    _friendly_names: Dict[str, uuid.UUID]
    _executor: ThreadPoolExecutor

    def __init__(self):
        self._actors = {}
        self._actor_locks = defaultdict(Lock)
        self._mailboxes = defaultdict(list)
        self._friendly_names = {}
        self._mailbox_semaphore = asyncio.Semaphore(1)
        self.loop = get_running_loop()
        self.loop.create_task(self._dispatch_messages(self.loop))
        self.loop.create_task(self._collect_messages(self.loop))
        self._executor = ThreadPoolExecutor(max_workers=10)

    async def _dispatch_messages(self, loop: AbstractEventLoop):
        while loop.is_running():
            async with self._mailbox_semaphore:
                for actor_id, box in self._mailboxes.items():
                    if len(box) > 0:
                        message = box.pop(0)
                        self._executor.submit(
                            self._dispatch_single_message,
                            actor_id,
                            message,
                            self._actor_locks[actor_id]
                        )
            await asyncio.sleep(0)

    async def _collect_messages(self, loop: AbstractEventLoop):
        while loop.is_running():
            for actor_id, actor in self._actors.items():
                for (to, msg) in actor.empty_mailbox():
                    self.loop.create_task(
                        self.send_message(to, msg)
                    )
            await asyncio.sleep(0)

    def _dispatch_single_message(self, actor_id, message, lock: Lock):
        try:
            with lock:
                self._actors[actor_id].handle_message(message)
        except Exception as e:
            logger.warning("Failed dispatching message", exc_info=e)

    def add_actor(self, actor: Actor):
        new_id = uuid.uuid4()
        self._actors[new_id] = actor
        if hasattr(actor, "name") and actor.name:
            self._friendly_names[actor.name] = new_id
        self.loop.create_task(self._start_actor(new_id))

    async def _start_actor(self, actor_id):
        self._actors[actor_id].start(actor_id)

    async def send_message(self, to, message):
        if hasattr(to, "id"):
            receiver_id = to.id
        elif isinstance(to, uuid.UUID):
            receiver_id = to
        elif isinstance(to, str) and to in self._friendly_names:
            receiver_id = self._friendly_names[to]
        else:
            logger.warning(f"Invalid message address: {to}")
            return

        async with self._mailbox_semaphore:
            self._mailboxes[receiver_id].append(message)


class BasicActor(ABC):
    __id: Optional[uuid.UUID]
    _outbox: List
    _name: Optional[str]

    def __init__(self, name=None):
        self._outbox = []
        self.__id = None
        self._name = name

    def start(self, assigned_id: uuid.UUID):
        self.__id = assigned_id
        self.started_event()

    @property
    def name(self):
        return self._name

    async def until_started(self):
        while not self.__id:
            await asyncio.sleep(0.5)

    @property
    def id(self):
        if not self.__id:
            raise Exception("Actor hasn't started yet")
        return self.__id

    def send_message(self, to, message):
        self._outbox.append((to, message))

    def empty_mailbox(self):
        msgs = self._outbox
        self._outbox = []
        return msgs

    @abstractmethod
    def started_event(self):
        pass

    @abstractmethod
    def handle_message(self, message):
        pass
