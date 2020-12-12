import asyncio
from asyncio import get_running_loop

from shakespyeare import BasicActor, Stage


async def main():
    stage = Stage()
    simple_actor = PrintsMessages("hello from one", name="one", forward="two")
    simple_actor_2 = PrintsMessages("hello from two", name="two", forward="one")
    stage.add_actor(simple_actor)
    stage.add_actor(simple_actor_2)
    await stage.send_message("one", "hello")
    while get_running_loop().is_running():
        await asyncio.sleep(10)


class PrintsMessages(BasicActor):
    def __init__(self, msg_content, name=None, forward=None):
        self.msg_content = msg_content
        self.forward = forward
        super().__init__(name)

    async def handle_message(self, message):
        print(message)
        if len(message) > 100:
            message = "TRUNCATED"
        await asyncio.sleep(1)
        if self.forward:
            self.send_message(self.forward, message + " & " + self.msg_content)

    async def started_event(self):
        print(f"started {self._name}")


if __name__ == '__main__':
    asyncio.run(main())


