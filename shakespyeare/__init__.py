import asyncio
import logging
import uuid
from abc import ABC, abstractmethod
from asyncio import get_running_loop, AbstractEventLoop
from collections import defaultdict
from typing import Dict, List, Any, Optional
from typing_extensions import Protocol

logger = logging.getLogger(__name__)


class MessageBroker(Protocol):
    async def send_message(self, to, message): ...


class Actor(Protocol):
    async def start(self, message_broker: MessageBroker, assigned_id: uuid.UUID): ...

    async def handle_message(self, message): ...

    def empty_mailbox(self) -> List: ...


class Stage(MessageBroker):
    _actors: Dict[uuid.UUID, Actor]
    _mailboxes: Dict[uuid.UUID, List[Any]]
    _mailbox_semaphore: asyncio.Semaphore

    def __init__(self):
        self._actors = {}
        self._mailboxes = defaultdict(list)
        self._mailbox_semaphore = asyncio.Semaphore(1)
        self.loop = get_running_loop()
        self.loop.create_task(self._dispatch_messages(self.loop))
        self.loop.create_task(self._collect_messages(self.loop))

    async def _dispatch_messages(self, loop: AbstractEventLoop):
        while loop.is_running():
            async with self._mailbox_semaphore:
                for actor_id, box in self._mailboxes.items():
                    if len(box) > 0:
                        message = box.pop(0)
                        self.loop.create_task(
                            self._dispatch_single_message(actor_id, message),
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

    async def _dispatch_single_message(self, actor_id, message):
        try:
            await self._actors[actor_id].handle_message(message)
        except Exception as e:
            logger.warning("Failed dispatching message", exc_info=e)

    def add_actor(self, actor: Actor):
        new_id = uuid.uuid4()
        self._actors[new_id] = actor
        self.loop.create_task(actor.start(self, new_id))

    async def send_message(self, to, message):
        if hasattr(to, "id"):
            receiver_id = to.id
        elif isinstance(to, uuid.UUID):
            receiver_id = to
        else:
            logger.warning(f"Invalid message address: {to}")
            return

        async with self._mailbox_semaphore:
            self._mailboxes[receiver_id].append(message)


class BasicActor(ABC):
    _message_broker: Optional[MessageBroker]
    __id: Optional[uuid.UUID]
    _outbox: List

    async def start(self, message_broker: MessageBroker, assigned_id: uuid.UUID):
        self._message_broker = message_broker
        self.__id = assigned_id
        self._outbox = []
        await self.started_event()

    @property
    def id(self):
        if not self.__id:
            raise Exception("Actor hasn't started yet")
        return self.__id

    def send_message(self, to, message):
        if self._outbox is None:
            raise Exception("Actor hasn't started yet")
        self._outbox.append((to, message))

    def empty_mailbox(self):
        if not hasattr(self, "_outbox"):
            return []
        msgs = self._outbox
        self._outbox = []
        return msgs

    @abstractmethod
    async def started_event(self):
        pass

    @abstractmethod
    async def handle_message(self, message):
        pass
