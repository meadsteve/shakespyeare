import asyncio
from asyncio import get_running_loop
from time import sleep

from shakespyeare import BasicActor, Stage


async def main():
    stage = Stage()
    simple_actor = PrintsMessages("hello from one", name="one", forward="two")
    simple_actor_2 = PrintsMessages("hello from two", name="two", forward="one")
    stage.add_actors(simple_actor, simple_actor_2)
    stage.send_message("one", "hello")
    while get_running_loop().is_running():
        await asyncio.sleep(10)


class PrintsMessages(BasicActor):
    def __init__(self, msg_content, name=None, forward=None):
        self.msg_content = msg_content
        self.forward = forward
        super().__init__(name)

    async def handle_message(self, message, sent_from, state):
        await asyncio.sleep(1)
        print(message)
        if len(message) > 100:
            message = "TRUNCATED"
        if self.forward:
            self.send_message(self.forward, message + " & " + self.msg_content)
        return state


if __name__ == '__main__':
    asyncio.run(main())


