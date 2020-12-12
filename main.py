import asyncio
from asyncio import get_running_loop

from shakespyeare import BasicActor, Stage


async def main():
    stage = Stage()
    simple_actor = PrintsMessages("hello from one")
    simple_actor_2 = PrintsMessages("hello from two")
    stage.add_actor(simple_actor)
    stage.add_actor(simple_actor_2)
    await stage.send_message(simple_actor, "hello")
    while get_running_loop().is_running():
        await asyncio.sleep(10)


class PrintsMessages(BasicActor):
    def __init__(self, msg_content):
        self.msg_content = msg_content

    async def handle_message(self, message):
        print(message)
        await asyncio.sleep(1)
        self.send_message(self.id, self.msg_content)

    async def started_event(self):
        print("started")
        await asyncio.sleep(1)
        self.send_message(self.id, "starting!!")


if __name__ == '__main__':
    asyncio.run(main())


